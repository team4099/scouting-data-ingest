from datetime import datetime

import gspread
import numpy
import pandas as pd
import requests
from loguru import logger
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
    Boolean,
    Float,
    null,
    DateTime,
)
from sqlalchemy.orm import relationship
import pytz
from DataAccessor import DataAccessor

from SQLObjects import (
    Alliance,
    Base,
    CompLevel,
    Match,
    Team,
    flatten_json,
    MatchDatum,
    TeamDatum,
    match_data_map,
)


# Main Input Object that will handle all the input
class DataInput:
    def __init__(
        self, engine, session, connection, data_accessor: DataAccessor, config
    ):
        # Get logger
        self.log = logger.opt(colors=True)

        self.log.info("Starting DataInput")
        # Loading configuration
        self.log.info("Loading Configuration")

        self.config = config

        # Connecting to MySQL
        self.log.info("Connecting to MySQL")
        self.engine = engine
        self.session = session
        self.connection = connection
        self.data_accessor = data_accessor
        self.event = self.config.year + self.config.event

        # Exists to use a year specific object types
        self.log.info("Initializing Variables")

        # Set as early as possible to make sure the first TBA response on load will provide data
        self.tba_last_modified = "Wed, 1 Jan 1000 00:00:01 GMT"
        self.sheet_last_modified = None
        self.last_tba_time = 0
        self.last_tba_match = None

        # Object to represent worksheet
        gc = gspread.service_account(f"./config/{self.config.google_credentials}")
        if self.config.simulation:
            self.sheet = gc.open(f"{self.config.simulator_spreadsheet}").get_worksheet(
                0
            )
        else:
            self.sheet = gc.open(f"{self.config.spreadsheet}").get_worksheet(0)

        self.log.info("Loading matches and teams")
        self.load_matches_and_teams()

        self.log.info("DataInput Loaded!")

    def get_tba_data(self):
        """

        Gets Data from TBA and places it in SQL.

        :param event: Event Key
        :type event: str
        :return: TBA request status code
        :rtype: int
        """
        self.log.info("Loading TBA Data")
        headers = {
            "X-TBA-Auth-Key": self.config.tba_key,
            "If-Modified-Since": self.tba_last_modified,
        }
        if self.config.simulation:
            url = f"{self.config.simulator_url}/matches"
        else:
            url = f"https://www.thebluealliance.com/api/v3/event/{self.event}/matches"

        r = requests.get(url, headers=headers)

        # Stop if we don't get a proper response
        if r.status_code != 200 and r.status_code != 304:
            self.log.error(
                f"Data not successfully retrieved with status code {r.status_code}"
            )
            return r.status_code
        elif r.status_code == 304:
            self.log.info("TBA has not been changed. It will not be updated.")
            return r.status_code
        self.log.info("Data successfully retrieved")
        self.tba_last_modified = r.headers["Last-Modified"]
        self.log.info("Normalizing and Cleaning Data")

        # Flatten the data and sort it so matches are entered in a sane way
        occurred_data = sorted(
            filter(lambda x: x["post_result_time"] > self.last_tba_time, r.json()),
            key=lambda x: x["post_result_time"],
        )
        self.last_tba_time = occurred_data[-1]["post_result_time"]
        self.last_tba_match = occurred_data[-1]["key"]
        matches = [flatten_json(i) for i in occurred_data]

        self.log.info("Adding Match Data")
        # Add matches
        for match in matches:
            self.data_accessor.add_match_datum(match["key"], match)

        self.session.commit()
        self.log.info("Finished getting TBA Data.")
        return r.status_code

    def get_sheet_data(self, event):
        """

        Gets Data from Google Sheets and places it in SQL.

        :param event: Name of Event
        :type event: str
        """
        self.log.info("Getting sheet data")
        self.config.check_internet_connection()
        if self.config.connected_to_internet:
            data = pd.DataFrame(self.sheet.get_all_records())
            # Write data to Excel file
        else:
            # Get data from sheet
            ...

        self.log.info("Data successfully retrieved")
        self.log.info("Getting Datatypes")
        data["Team Number"] = "frc" + data["Team Number"].astype(str)
        data["Match Key"] = (
            self.config.year + self.config.event + "_" + data["Match Key"].astype(str)
        )
        #data["Climb Type"] = data["Climb Type"].str.lower()
        # Get rid of empty cells/ and spaces with dropna and then use SQLAlchemy null() for it to show as null in mySQL
        data = data.replace(r"^\s*$", numpy.nan, regex=True)
        data = data.replace("Yes", 1)
        data = data.replace("No", 0)
        data.astype(data.dropna().infer_objects().dtypes)
        data = data.replace(numpy.nan, null(), regex=True)

        # If the sheet hasn't been modified, do nothing
        if (
            self.sheet_last_modified is not None
            and datetime.strptime(
                data.iloc[-1:]["Timestamp"].iloc[0], "%m/%d/%Y %H:%M:%S"
            )
            <= self.sheet_last_modified
        ):
            self.log.info(
                "The sheet has not been modified. The data will not be updated."
            )
            return

        self.log.info("Clearing Old Data")
        # self.data_accessor.delete_team_data()
        self.log.info("Adding Team Data")
        # Format Data

        # Add Data
        for team_datum in data.to_dict(orient="records"):
            self.data_accessor.add_team_datum(
                team_id=team_datum["Team Number"],
                match_id=team_datum["Match Key"],
                alliance=Alliance(team_datum["Alliance"].lower()),
                driver_station=team_datum["Driver Station"],
                team_datum_json=team_datum,
            )
        self.log.info("Committing changes")
        self.session.commit()
        self.sheet_last_modified = datetime.strptime(
            data.iloc[-1:]["Timestamp"].iloc[0], "%m/%d/%Y %H:%M:%S"
        )
        self.log.info("Finished getting sheet data")

    def load_matches_and_teams(self):
        self.log.info("Loading TBA Data")

        headers = {
            "X-TBA-Auth-Key": self.config.tba_key,
        }
        if self.config.simulation:
            match_url = f"{self.config.simulator_url}/matches"
            team_url = f"{self.config.simulator_url}/teams"
        else:
            match_url = (
                f"https://www.thebluealliance.com/api/v3/event/{self.event}/matches"
            )
            team_url = (
                f"https://www.thebluealliance.com/api/v3/event/{self.event}/teams/keys"
            )

        match_r = requests.get(match_url, headers=headers)
        team_r = requests.get(team_url, headers=headers)

        # Stop if we don't get a proper response
        if (
            match_r.status_code != 200
            and match_r.status_code != 304
            and team_r.status_code != 200
            and team_r.status_code != 304
        ):
            self.log.error(
                f"Data not successfully retrieved with status code {match_r.status_code} and {team_r.status_code}"
            )
            return [match_r.status_code, team_r.status_code]

        self.log.info("Data successfully retrieved")
        self.log.info("Adding Matches")

        for match in match_r.json():
            self.data_accessor.add_match(
                match["key"],
                CompLevel(match["comp_level"]),
                match["set_number"],
                match["match_number"],
                match["event_key"],
            )

        for team in team_r.json():
            self.data_accessor.add_team(team)
