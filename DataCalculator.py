import json

import numpy
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, null
from sqlalchemy.orm import sessionmaker, relationship

from SQLObjects import Base, Teams
from terminal import console, logger
import pandas as pd
from re import search


class DataCalculator:
    def __init__(self, engine, session, connection, dataAccessor):
        self.log = logger

        self.log.info("[bold green]Starting [bold white]DataCalculator")
        # Loading configuration
        self.log.info("[bold white]Loading Configuration")
        with open("config/config.json") as f:
            config = json.load(f)

        self.config = config

        # Connecting to MySQL
        self.log.info("[bold white]Connecting to MySQL")
        self.engine = engine
        self.session = session
        self.connection = connection
        self.dataAccessor = dataAccessor

        self.log.info("[bold white]Initializing Variables")
        self.team_list = pd.DataFrame()

        self.log.info("[bold white]DataCalculator Loaded!")

    def calculate_team_average(self, col):
        team_data = self.dataAccessor.getTeamData()[['teamid', col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(team_data.groupby('teamid').mean(), how='outer', left_on='id',
                                                     right_index=True)
        else:
            team_data_average = self.team_list.merge(team_data, how='outer', left_on='id', right_index=True).drop(
                columns=['teamid'])
        team_data_average = team_data_average.rename(columns={col: col + "_avg"})
        return team_data_average.set_index('id')

    def calculate_team_median(self, col):
        team_data = self.dataAccessor.getTeamData()[['teamid', col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(team_data.groupby('teamid').median(), how='outer', left_on='id',
                                                     right_index=True)
        else:
            team_data_average = self.team_list.merge(team_data, how='outer', left_on='id', right_index=True).drop(
                columns=['teamid'])
        team_data_average = team_data_average.rename(columns={col: col + "_med"})
        return team_data_average.set_index('id')

    def calculate_team_percentages(self, cols, one_hot_encoded=True, replacements=None):
        if replacements is None:
            replacements = {}
        team_data = self.dataAccessor.getTeamData()[[*cols, 'teamid']]
        if not one_hot_encoded:
            team_data_encoded = pd.get_dummies(team_data[cols])
            team_data[team_data_encoded.columns] = team_data_encoded[team_data_encoded.columns]
        if replacements is not None:
            for k, v in replacements.items():
                team_data = team_data.replace(k, v)
        counts = team_data.dropna().groupby('teamid').mean()
        team_data_percentages = self.team_list.merge(counts, how='outer', left_on='id', right_index=True)
        team_data_percentages = team_data_percentages.rename(columns={c: c + "_pct" for c in cols})
        return team_data_percentages.set_index('id')

    def team_data_sql_config(self, df):
        calc_data_config = {}
        df = df.reset_index().rename(columns={'id': 'teamid'})
        df = df.infer_objects()
        for col, dtype in zip(df.columns, df.dtypes):
            if dtype == numpy.float64:
                calc_data_config[col] = f"Column(Float)"
            elif dtype == numpy.int64:
                calc_data_config[col] = f"Column(Integer)"
            elif dtype == numpy.object:
                calc_data_config[col] = f"Column(String(100))"
            elif dtype == numpy.bool:
                calc_data_config[col] = f"Column(Boolean())"
            else:
                self.log.warning(
                    f"{dtype} is not a configured datatype. It will not be used."
                )
        t_data = {
            "__tablename__": f'CalculatedTeamData{self.config["Year"]}',
            "__table_args__": {"extend_existing": True},
            "id": Column(Integer, primary_key=True),
            #            "teamid": Column(String(50), ForeignKey("team.id")),

        }

        calc_data_config = {
            k: eval(v) for k, v in calc_data_config.items()
        }
        self.CalculatedTeamDataObject = type(
            f'CalculatedTeamData{self.config["Year"]}',
            (Base,),
            {**calc_data_config, **t_data},
        )
        self.dataAccessor.CalculatedTeamDataObject = self.CalculatedTeamDataObject
        #        Teams.calc_data = relationship(f'CalculatedTeamData{self.config["Year"]}')
        self.session.commit()
        Base.metadata.tables[f'CalculatedTeamData{self.config["Year"]}'].create(bind=self.engine)
        self.session.commit()

    def team_data_to_sql(self, dfs):
        self.log.info('[bold white]Joining Dataframes')
        full_df = dfs[0].join(dfs[1:])
        self.log.info('[bold white]Configuring SQL')
        self.team_data_sql_config(full_df)
        self.log.info('[bold white]Configured SQL')

        self.log.info('[bold white]Adding Data')
        # TODO: Raise Issue about error when you comment following line
        full_df = full_df.replace(numpy.nan, null(), regex=True)
        for row in full_df.iterrows():
            self.dataAccessor.addCalculatedTeamData(row[0], row[1])
        self.session.commit()

    def calculate_team_data(self):
        self.log.info("[bold white]Getting a team list")
        self.team_list = self.dataAccessor.getTeams()

        self.log.info("[bold white]Calculating averages")
        auto_low_avg = self.calculate_team_average("Cells_scored_in_Low_Goal")
        auto_high_avg = self.calculate_team_average("Cells_scored_in_High_Goal")
        tele_low_avg = self.calculate_team_average("Low_Goal")
        tele_high_avg = self.calculate_team_average("High_Goal")
        tele_miss_avg = self.calculate_team_average("Misses")
        fouls_avg = self.calculate_team_average("Fouls")
        climb_time_avg = self.calculate_team_average("Climb_Time")

        self.log.info("[bold white]Calculating medians")
        auto_low_med  = self.calculate_team_median("Cells_scored_in_Low_Goal")
        auto_high_med = self.calculate_team_median("Cells_scored_in_High_Goal")
        tele_low_med  = self.calculate_team_median("Low_Goal")
        tele_high_med = self.calculate_team_median("High_Goal")
        tele_miss_med = self.calculate_team_median("Misses")
        fouls_med = self.calculate_team_median("Fouls")
        climb_time_med = self.calculate_team_median("Climb_Time")

        self.log.info("[bold white]Calculating percentages")
        shooting_zone_pct = self.calculate_team_percentages(
            ['Target_Zone?', 'Initiation_Line?', 'Near_Trench?', 'Rendezvous_point?', 'Far_Trench'],
            replacements={"Yes": 1, "No": 0})
        climb_type_pct = self.calculate_team_percentages(['Climb_Type'], one_hot_encoded=False)

        self.log.info("[bold white]Adding data to SQL")
        self.team_data_to_sql(
            [
                auto_low_avg,
                auto_high_avg,
                tele_low_avg,
                tele_high_avg,
                tele_miss_avg,
                fouls_avg,
                climb_time_avg,
                auto_low_med,
                auto_high_med,
                tele_low_med,
                tele_high_med,
                tele_miss_med,
                fouls_med,
                climb_time_med,
                shooting_zone_pct,
                climb_type_pct
            ]
        )
        # OPR (xOPR or ixOPR)
        # Consistency scores
        # ELO
