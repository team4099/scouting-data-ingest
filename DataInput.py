import gspread
from sqlalchemy import create_engine, Table, exists, null
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    BigInteger,
    ForeignKey,
    Integer,
    String,
    Boolean,
    Float,
    Text,
)
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import text
import json
import numpy
import requests
import pandas as pd
from warnings import filterwarnings
from datetime import datetime

from terminal import console, logger
import re
from SQLObjects import Matches, Teams, Base


# Main Input Object that will handle all the input
class DataInput:
    def __init__(self, engine, session, connection, dataAccessor):
        # Get logger
        self.log = logger

        self.log.info("[bold green]Starting [bold yellow]DataInput")
        # Loading configuration
        self.log.info("[bold yellow]Loading Configuration")
        with open("config/config.json") as f:
            config = json.load(f)

        self.config = config

        # Connecting to MySQL
        self.log.info("[bold yellow]Connecting to MySQL")
        self.engine = engine
        self.session = session
        self.connection = connection
        self.dataAccessor = dataAccessor

        # Erasing old data to ensure proper column set up


        # Exists to use a year specific object types
        self.log.info("[bold yellow]Initializing Variables")
        self.TeamDataObject = None
        self.MatchDataObject = None

        # Set as early as possible to make sure the first TBA response on load will provide data
        self.tbaLastModified = "Wed, 1 Jan 100 00:00:01 GMT"
        self.sheetLastModified = None

        # Object to represent worksheet
        self.sheet = None

        # Reads config files and sets up variables and SQL from them
        self.parseConfig()

        # Creates everything and puts into SQL
        self.log.info("[bold yellow]Creating ORM Objects")
        Base.metadata.create_all(self.engine)

        self.session.commit()
        self.log.info("[bold yellow]DataInput Loaded!")

    def getTBAData(self, event: str):
        self.log.info("[bold yellow]Loading TBA Data")
        headers = {
            "X-TBA-Auth-Key": self.config["TBA-Key"],
            "If-Modified-Since": self.tbaLastModified,
        }
        r = requests.get(
            f"https://www.thebluealliance.com/api/v3/event/{event}/matches",
            headers=headers,
        )
        if r.status_code != 200:
            self.log.error(
                f"Data not successfully retrieved with status code {r.status_code}"
            )
            return r.status_code
        self.log.info("[bold yellow]Data successfully retrieved")
        self.tbaLastModified = r.headers["Last-Modified"]
        self.log.info("[bold yellow]Normalizing and Cleaning Data")
        data = pd.json_normalize(r.json())
        data = data.sort_values(by="actual_time")
        drop_list = ["videos", "score_breakdown"]
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                pass
        self.log.info("[bold yellow]Getting Datatypes")
        data = data.infer_objects()
        # blue_keys = data["alliances.blue.team_keys"]
        # red_keys = data["alliances.red.team_keys"]
        # split_list = [
        #     "alliances.blue.dq_team_keys",
        #     "alliances.blue.team_keys",
        #     "alliances.blue.surrogate_team_keys",
        #     "alliances.red.dq_team_keys",
        #     "alliances.red.team_keys",
        #     "alliances.red.surrogate_team_keys",
        # ]
        # for split_c in split_list:
        #     data[[split_c + ".1", split_c + ".2", split_c + ".3"]] = [
        #         [k[i] if i < len(k) else None for i in range(3)] for k in data[split_c]
        #     ]
        drop_list = [
            "videos",
            "score_breakdown",
            "alliances.blue.dq_team_keys",
            "alliances.blue.team_keys",
            "alliances.blue.surrogate_team_keys",
            "alliances.red.dq_team_keys",
            "alliances.red.team_keys",
            "alliances.red.surrogate_team_keys",
        ]
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                pass
        self.log.info("[bold yellow]Adding Matches")
        for row in data.iterrows():
            x = row[1]
            self.dataAccessor.addMatchData(x["key"], self.MatchDataObject(**x.to_dict()))
        self.session.commit()
        self.log.info("[bold yellow]Finished getting TBA Data.")
        return r.status_code

    def getSheetData(self, eventName):
        self.log.info("[bold yellow]Getting sheet data")
        data = pd.DataFrame(self.sheet.get_all_records())
        self.log.info("[bold yellow]Data successfully retrieved")
        self.log.info("[bold yellow]Getting Datatypes")
        data = data.replace(r"^\s*$", numpy.nan, regex=True)
        data.astype(data.dropna().infer_objects().dtypes)
        data = data.replace(numpy.nan, null(), regex=True)
        data.columns = [i.replace(" ", "_") for i in data.columns]
        if self.sheetLastModified is None:
            pass
        elif (
                datetime.strptime(data.iloc[-1:]["Timestamp"][0], "%m/%d/%Y %H:%M:%S")
                > self.sheetLastModified
        ):
            return
        self.log.info("[bold yellow]Adding Team Data")
        for row in data.iterrows():
            x = row[1]
            for d in ["Team_Number"]:
                try:
                    x = x.drop(labels=[d])
                except KeyError:
                    pass
            x["Match_Key"] = eventName + "_" + x["Match_Key"]
            if not self.dataAccessor.checkIfTeamExists(f'frc{row[1]["Team_Number"]}'):
                self.dataAccessor.addTeam(f'frc{row[1]["Team_Number"]}')
                self.session.commit()

            if not self.dataAccessor.checkIfTeamDataExists(
                    f'frc{row[1]["Team_Number"]}', x["Match_Key"]
            ):
                teams_in_match = data[(eventName + "_" + data['Match_Key'])==x['Match_Key']]
                red_teams = teams_in_match[teams_in_match['Alliance']=='Red']['Team_Number'].tolist()
                blue_teams = teams_in_match[teams_in_match['Alliance']=='Blue']['Team_Number'].tolist()
                if row[1]["Team_Number"] in red_teams:
                    x["Alliance"] = "Red"
                elif row[1]["Team_Number"] in blue_teams:
                    x["Alliance"] = "Blue"
                else:
                    raise Exception(f"Invalid Team Number {row[1]['Team_Number']} in match {x['Match_Key']}")
                self.dataAccessor.addTeamData(f'frc{row[1]["Team_Number"]}',x['Match_Key'],x)
            else:
                self.log.warning("This TeamData already exists. It will not be added.")
        self.log.info("[bold yellow]Commiting changes")
        self.session.commit()
        self.sheetLastModified = datetime.strptime(
            data.iloc[-1:]["Timestamp"].iloc[0], "%m/%d/%Y %H:%M:%S"
        )
        self.log.info("[bold yellow]Finished getting sheet data")

    def parseConfig(self):

        headers = {
            "X-TBA-Auth-Key": self.config["TBA-Key"],
            "If-Modified-Since": self.tbaLastModified,
        }
        self.log.info("[bold yellow]Getting TBA Data")
        r = requests.get(
            f'https://www.thebluealliance.com/api/v3/event/{self.config["Year"]}vahay/matches',
            headers=headers,
        )
        self.log.info("[bold yellow]Cleaning and Preparing data")
        data = pd.json_normalize(r.json())
        split_list = [
            "alliances.blue.dq_team_keys",
            "alliances.blue.team_keys",
            "alliances.blue.surrogate_team_keys",
            "alliances.red.dq_team_keys",
            "alliances.red.team_keys",
            "alliances.red.surrogate_team_keys",
        ]
        for split_c in split_list:
            data[[split_c + ".1", split_c + ".2", split_c + ".3"]] = [
                [k[i] if i < len(k) else None for i in range(3)] for k in data[split_c]
            ]
        drop_list = [
            "videos",
            "score_breakdown",
            "alliances.blue.dq_team_keys",
            "alliances.blue.team_keys",
            "alliances.blue.surrogate_team_keys",
            "alliances.red.dq_team_keys",
            "alliances.red.team_keys",
            "alliances.red.surrogate_team_keys",
        ]
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                passdata = data.infer_objects()
        self.log.info("[bold yellow]Constructing Configuration")
        matchDataConfig = {}
        for col, dtype in zip(data.columns, data.dtypes):
            if dtype == numpy.float64:
                matchDataConfig[col] = f"Column(Float)"
            elif dtype == numpy.int64:
                matchDataConfig[col] = f"Column(Integer)"
            elif dtype == numpy.object:
                matchDataConfig[col] = f"Column(String(100))"
            elif dtype == numpy.bool:
                matchDataConfig[col] = f"Column(Boolean())"
            else:
                self.log.warning(
                    f"{dtype} is not a configured datatype. It will not be used."
                )
        self.log.info("[bold yellow]Getting sheet data")
        gc = gspread.service_account(f'./config/{self.config["Google-Credentials"]}')
        self.sheet = gc.open(f'{self.config["Spreadsheet"]}').get_worksheet(0)
        self.log.info("[bold yellow]Cleaning and Preparing Data")
        data = pd.DataFrame(self.sheet.get_all_records())
        drop_list = ["Team Number"]
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                pass
        data.columns = [i.replace(" ", "_") for i in data.columns]
        data = data.replace(r"^\s*$", numpy.nan, regex=True)
        data.astype(data.dropna().infer_objects().dtypes)
        self.log.info("[bold yellow]Constructing Configuration")
        teamDataConfig = {
            "Alliance": "Column(Text(20))"
        }
        for col, dtype in zip(data.columns, data.dtypes):
            if dtype == numpy.int64 or dtype == numpy.float64:
                teamDataConfig[col] = f"Column(Float())"
            elif dtype == numpy.object:
                teamDataConfig[col] = f"Column(Text(500))"
            elif dtype == numpy.bool:
                teamDataConfig[col] = f"Column(Boolean())"
            else:
                self.log.warning(
                    f"{dtype} is not a configured datatype. It will not be used."
                )
        SQLConfig = {
            "TeamDataConfig": {
                "Year": self.config["Year"],
                "Attributes": teamDataConfig,
            },
            "MatchDataConfig": {
                "Year": self.config["Year"],
                "Attributes": matchDataConfig,
            },
        }
        self.log.info("[bold yellow]Configuring SQL")
        self.parseSQLConfig(SQLConfig)

    def parseSQLConfig(self, SQLconfig):
        self.log.info("[bold yellow]Constructing TeamData Object")
        t_data = {
            "__tablename__": f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}',
            "__table_args__": {"extend_existing": True},
            "id": Column(Integer, primary_key=True),
            "teamid": Column(String(50), ForeignKey("team.id")),
        }

        SQLconfig["TeamDataConfig"]["Attributes"] = {
            k: eval(v) for k, v in SQLconfig["TeamDataConfig"]["Attributes"].items()
        }
        self.TeamDataObject = type(
            f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}',
            (Base,),
            {**SQLconfig["TeamDataConfig"]["Attributes"], **t_data},
        )
        self.dataAccessor.TeamDataObject = self.TeamDataObject
        Teams.data_list = relationship(f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}')

        self.log.info("[bold yellow]Constructing MatchData Object")
        m_data = {
            "__tablename__": f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}',
            "__table_args__": {"extend_existing": True},
            "id": Column(Integer, primary_key=True),
            "matchId": Column(String(50), ForeignKey("match.id")),
        }

        SQLconfig["MatchDataConfig"]["Attributes"] = {
            k: eval(v) for k, v in SQLconfig["MatchDataConfig"]["Attributes"].items()
        }
        self.MatchDataObject = type(
            f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}',
            (Base,),
            {**SQLconfig["MatchDataConfig"]["Attributes"], **m_data},
        )
        self.dataAccessor.MatchDataObject = self.MatchDataObject
        Matches.data_list = relationship(
            f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}', uselist=False
        )
