import json
from sqlalchemy import create_engine, Table, exists, null
from sqlalchemy.orm import sessionmaker
from terminal import console
import pandas as pd
from re import search
import logging
from rich.logging import RichHandler

class DataProcessor:
    def __init__(self):
        with console.status("Setting Up DataProcessor") as status:
            # Loading configuration
            console.log("Loading Configuration")
            with open('config/config.json') as f:
                config = json.load(f)

            self.config = config

            # Connecting to MySQL
            console.log("Connecting to MySQL")
            self.engine = create_engine(
                f'mysql+pymysql://{self.config["Database User"]}:{self.config["Database Password"]}@localhost/scouting')
            self.Sessiontemplate = sessionmaker()
            self.Sessiontemplate.configure(bind=self.engine)
            self.session = self.Sessiontemplate()
            self.connection = self.engine.connect()

            console.log("Initializing Variables")
            self.warning_dict = {}
            self.last_checked = None

            logging.basicConfig(level="NOTSET", format="%(message)s", datefmt="[%X]",handlers=[RichHandler(markup=True)])
            self.log = logging.getLogger("rich")

    #TODO: Figure out proper Equals behavior regarding different alliances
    def checkEquals(self, team_data_columns, match_data_columns, weights=None): # iterable of series, series
        warnings = {}
        if weights is None:
            weights = [1 for i in range(len(team_data_columns))]
        sumTeamColumn = pd.Series()
        for column, weight in zip(team_data_columns):
            sumTeamColumn += column * weight

        sumMatchColumn = pd.Series()
        for column in match_data_columns:
            sumMatchColumn += column

        for (t_ind,t_val),(m_ind,m_val) in zip(sumTeamColumn.iteritems(), sumMatchColumn.iteritems()):
            if t_val != m_val:
                if len(weights) > 1:
                    warning = f"At index {t_ind}, the TeamData{self.config['Year']} {team_data_columns[0].name} value and the MatchData{self.config['Year']} {match_data_columns[0].name} do not match"
                self.log.warning(warning)
                warnings.append(warning)


    def checkSame(self, team_data_column, match_data_column): # series, series
        pass

    def checkExists(self, team_data_column, match_data_column): # series, series
        pass

    def checkKey(self, team_data_column): #series
        warnings = []
        for index,key in team_data_column.iteritems():
            if not search(r"2020[a-z]{4,5}_(qm|sf|qf|f)\d{1,2}(m\d{1})*",key):
                warning = f"Match Key in TeamData2020 at index {index} is not a proper key"
                self.log.warning(warning)
                warnings.append(warning)
        return warnings

    def checkCondition(self, column, condition, name): #series
        pass

    def checkData(self):
        warnings = {}
        with console.status("[bold green]Validating Data...") as status:
            console.log("Loading Data")
            team_data = pd.read_sql_table(f"teamdata{self.config['Year']}",self.connection)
            match_data = pd.read_sql_table(f"matchdata{self.config['Year']}",self.connection)

            console.log("Checking TeamData match keys")
            warnings['Match Key Violations'] = self.checkKey(team_data['Match_Key'])

        return warnings
