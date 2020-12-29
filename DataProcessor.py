import json
from sqlalchemy import create_engine, Table, exists, null
from sqlalchemy.orm import sessionmaker
from terminal import console,logger
import pandas as pd
from re import search


class DataProcessor:
    def __init__(self):
        console.log("[bold green]Setting Up DataProcessor")
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

        self.log = logger

    def checkEqualsByAlliance(self, team_data_columns, match_data_columns, team_weights=None,match_weights = None): # iterable of series, series
        warnings = []
        red_association = pd.read_sql_table('red_association',self.connection)
        blue_association = pd.read_sql_table('blue_association',self.connection)

        if team_weights is None:
            team_weights = [1 for i in range(len([col for col in team_data_columns.columns if col != 'Match_Key' and col != 'teamid']))]
        if match_weights is None:
            match_weights = [1 for i in range(len([col for col in match_data_columns['Blue'].columns if col != 'matchId']))]
        sumTeamColumn = pd.Series([0 for i in range(len(team_data_columns.index))])
        for column,weight in zip([col for col in team_data_columns.columns if col != 'Match_Key' and col != 'teamid'], team_weights):
            sumTeamColumn += team_data_columns[column] * weight
        team_data_columns['Sum'] = sumTeamColumn

        for color, data in match_data_columns.items():
            sumMatchColumn = pd.Series([0 for i in range(len(data.index))])
            for column,weight in zip([col for col in data.columns if col != 'matchId'], match_weights):
                sumMatchColumn += data[column] * weight
            data['Sum'] = sumMatchColumn
            match_data_columns[color] = data

        for color,data in match_data_columns.items():
            for index, row in data.iterrows():
                curr_match_data = team_data_columns.loc[team_data_columns['Match_Key'] == row['matchId']]
                if len(curr_match_data.index) == 0:
                    continue

                if color == 'Red':
                    curr_alliance_data = curr_match_data[
                        curr_match_data['teamid'].isin(
                            red_association[red_association['match_id'] == row['matchId']]['team_id']
                        )
                    ]
                elif color == 'Blue':
                    curr_alliance_data = curr_match_data[
                        curr_match_data['teamid'].isin(
                            blue_association[blue_association['match_id'] == row['matchId']]['team_id']
                        )
                    ]
                else:
                    self.log.warning(f"Color {color} is not valid")

                if row['Sum'] != curr_alliance_data['Sum'].sum():
                    warning = f'For the {color} alliance in match {row["matchId"]}, the {", ".join([col for col in team_data_columns.columns if col not in ["Match_Key","teamid","Sum"]])} columns do not equal the {", ".join([col for col in match_data_columns["Blue"].columns if col not in ["matchId","teamid","Sum"]])} columns'
                    self.log.warning(warning)
                    warnings.append(warning)

        return warnings


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
        console.log("[bold green]Validating Data...")
        console.log("Loading Data")
        team_data = pd.read_sql_table(f"teamdata{self.config['Year']}",self.connection)
        match_data = pd.read_sql_table(f"matchdata{self.config['Year']}",self.connection)

        console.log("Checking TeamData match keys")
        warnings['Match Key Violations'] = self.checkKey(team_data['Match_Key'])

        console.log("Checking for Auto Power Cell Low Goal Violations")
        warnings['Auto Power Cell Low Goal Violations'] = self.checkEqualsByAlliance(
            team_data.loc[:,['teamid','Match_Key','Cells_scored_in_Low_Goal']],
            {
                'Blue': match_data.loc[:,['matchId','score_breakdown.blue.autoCellsBottom']],
                'Red': match_data.loc[:,['matchId','score_breakdown.red.autoCellsBottom']]
            }
        )

        console.log("Checking for Auto Power Cell High Goal Violations")
        warnings['Auto Power Cell High Goal Violations'] = self.checkEqualsByAlliance(
            team_data.loc[:, ['teamid', 'Match_Key', 'Cells_scored_in_High_Goal']],
            {
                'Blue': match_data.loc[:, ['matchId', 'score_breakdown.blue.autoCellsInner', 'score_breakdown.blue.autoCellsOuter']],
                'Red': match_data.loc[:, ['matchId', 'score_breakdown.red.autoCellsInner', 'score_breakdown.red.autoCellsOuter']]
            }
        )

        console.log("Checking for Teleop Power Cell Low Goal Violations")
        warnings['Teleop Power Cell Low Goal Violations'] = self.checkEqualsByAlliance(
            team_data.loc[:, ['teamid', 'Match_Key', 'Low_Goal']],
            {
                'Blue': match_data.loc[:, ['matchId', 'score_breakdown.blue.teleopCellsBottom']],
                'Red': match_data.loc[:, ['matchId', 'score_breakdown.red.teleopCellsBottom']]
            }
        )

        console.log("Checking for Teleop Power Cell High Goal Violations")
        warnings['Teleop Power Cell High Goal Violations'] = self.checkEqualsByAlliance(
            team_data.loc[:, ['teamid', 'Match_Key', 'High_Goal']],
            {
                'Blue': match_data.loc[:,
                        ['matchId', 'score_breakdown.blue.teleopCellsInner', 'score_breakdown.blue.teleopCellsOuter']],
                'Red': match_data.loc[:,
                       ['matchId', 'score_breakdown.red.teleopCellsInner', 'score_breakdown.red.teleopCellsOuter']]
            }
        )


        return warnings
