import json

import numpy
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, null
from sqlalchemy.orm import sessionmaker, relationship

from SQLObjects import Base, Teams
from terminal import console, logger
import pandas as pd
from re import search
from sklearn.preprocessing import MultiLabelBinarizer
from scipy.sparse.linalg import lsmr


class DataCalculator:
    def __init__(self, engine, session, connection, data_accessor, config):
        self.log = logger.opt(colors=True)
        
        self.log.info("Starting DataCalculator")
        # Loading configuration
        self.log.info("Loading Configuration")

        self.config = config

        # Connecting to MySQL
        self.log.info("Connecting to MySQL")
        self.engine = engine
        self.session = session
        self.connection = connection

        self.log.info("Initializing Variables")
        self.team_list = pd.DataFrame()
        self.data_accessor = data_accessor
        self.calculated_team_data_object = None
        self.sql_configured = False

        self.log.info("DataCalculator Loaded!")

    def calculate_team_average(self, col):
        """

        Calculates an average by team for a metric.

        :param col: A metric column from TeamData.
        :type col: str
        :return: A Dataframe of averages
        :rtype: pandas.DataFrame
        """
        team_data = self.data_accessor.get_team_data()[['teamid', col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(team_data.groupby('teamid').mean(), how='outer', left_on='id',
                                                     right_index=True)
        else:
            team_data_average = self.team_list.merge(team_data, how='outer', left_on='id', right_index=True).drop(
                columns=['teamid'])
        team_data_average = team_data_average.rename(columns={col: col + "_avg"})
        return team_data_average.set_index('id')

    def calculate_team_median(self, col):
        """

        Calculates an median by team for a metric.

        :param col: A metric column from TeamData.
        :type col: str
        :return: A Dataframe of medians
        :rtype: pandas.DataFrame
        """
        team_data = self.data_accessor.get_team_data()[['teamid', col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(team_data.groupby('teamid').median(), how='outer', left_on='id',
                                                     right_index=True)
        else:
            team_data_average = self.team_list.merge(team_data, how='outer', left_on='id', right_index=True).drop(
                columns=['teamid'])
        team_data_average = team_data_average.rename(columns={col: col + "_med"})
        return team_data_average.set_index('id')

    def calculate_team_percentages(self, cols, one_hot_encoded=True, replacements=None, possible_values=None):
        """

        Calculates percentages by team for a metric.

        :param cols: Columns to convert to percentages
        :type cols: List[str]
        :param one_hot_encoded: Whether the data is already one hot encoded
        :type one_hot_encoded: Union[bool, None]
        :param replacements: A dict to map string values to integers
        :type replacements: Union[Dict[str, int], None]
        :type possible_values: A list of possible values if using one-hot encoding
        :type possible_values: Union[List[str], None]
        :return: A Dataframe of percentages
        :rtype: pandas.DataFrame
        """
        if replacements is None:
            replacements = {}
        team_data = self.data_accessor.get_team_data()[[*cols, 'teamid']]
        if not one_hot_encoded:
            team_data_encoded = pd.get_dummies(team_data[cols])
            team_data[team_data_encoded.columns] = team_data_encoded[team_data_encoded.columns]
            unused_col_names = [f"{col}_{name}" for col in cols for name in possible_values if f"{col}_{name}" not in team_data_encoded.columns ]
            team_data[unused_col_names] = pd.DataFrame(data=0, index=team_data_encoded.index, columns=unused_col_names)
        if replacements is not None:
            for k, v in replacements.items():
                team_data = team_data.replace(k, v)
        counts = team_data.dropna().groupby('teamid').mean()
        team_data_percentages = self.team_list.merge(counts, how='outer', left_on='id', right_index=True)
        team_data_percentages = team_data_percentages.rename(columns={c: c + "_pct" for c in cols})
        return team_data_percentages.set_index('id')

    def calculated_team_data_sql_config(self, df):
        """

        Configures the database for calculated data.

        :param df: Calculated Team Data
        :type df: pandas.DataFrame
        """
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
            "__tablename__": f'CalculatedTeamData{self.config.year}',
            "__table_args__": {"extend_existing": True},
        }

        calc_data_config['teamid'] = "Column(String(20), primary_key=True)"

        calc_data_config = {
            k: eval(v) for k, v in calc_data_config.items()
        }
        self.calculated_team_data_object = type(
            f'CalculatedTeamData{self.config.year}',
            (Base,),
            {**calc_data_config, **t_data},
        )
        self.data_accessor.CalculatedTeamDataObject = self.calculated_team_data_object
        self.session.flush()
        Base.metadata.tables[f'CalculatedTeamData{self.config.year}'].create(bind=self.engine)
        self.session.commit()
        self.sql_configured = True

    def team_data_to_sql(self, dfs):
        self.log.info('Joining Dataframes')
        full_df = dfs[0].join(dfs[1:])
        if not self.sql_configured:
            self.log.info('Configuring SQL')
            self.calculated_team_data_sql_config(full_df)
            self.log.info('Configured SQL')
        else:
            self.data_accessor.delete_calculated_team_data()

        self.log.info('Adding Data')
        full_df = full_df.replace(numpy.nan, null(), regex=True)
        for row in full_df.iterrows():
            self.data_accessor.add_calculated_team_data(row[0], row[1])
        self.session.commit()

    def calculate_opr(self, metric):
        team_lists = self.data_accessor.get_teams_in_match()
        team_lists.index = team_lists.index.sortlevel(1, ascending=True, sort_remaining=True)[0]
        team_lists = pd.DataFrame(team_lists).reset_index().rename({"Match_Key":"matchId"},axis=1)
        metric_data = self.data_accessor.get_match_data(type_df=True)[[f"score_breakdown.red.{metric}",f"score_breakdown.blue.{metric}","matchId"]].sort_values(by="matchId")
        blue_data = metric_data[[f"score_breakdown.blue.{metric}","matchId"]].rename({f"score_breakdown.blue.{metric}":metric},axis=1)
        red_data = metric_data[[f"score_breakdown.red.{metric}","matchId"]].rename({f"score_breakdown.red.{metric}":metric},axis=1)
        blue_data["Alliance"] = "Blue"
        red_data["Alliance"] = "Red"
        merged_data = blue_data.append(red_data)

        assembled_data = pd.merge(team_lists,merged_data,on=["matchId","Alliance"], how="inner")[["teamid",metric]]
        mlb = MultiLabelBinarizer(sparse_output=True)
        sparse_teams = mlb.fit_transform(assembled_data["teamid"])
        oprs = lsmr(sparse_teams,assembled_data[metric].to_numpy())
        teams_with_oprs = pd.DataFrame([mlb.classes_,oprs[0]]).transpose()
        teams_with_oprs.rename({0:"teams",1:f"{metric}_opr"},inplace=True,axis=1)
        teams_with_oprs.set_index("teams",inplace=True)
        return teams_with_oprs

    def calculate_team_data(self):
        """
            Calculates Team Data
        """
        self.log.info("Getting a team list")
        self.team_list = self.data_accessor.get_teams()

        self.log.info("Calculating averages")
        auto_low_avg = self.calculate_team_average("Auto_Low_Goal")
        auto_high_avg = self.calculate_team_average("Auto_High_Goal")
        tele_low_avg = self.calculate_team_average("Teleop_Low_Goal")
        tele_high_avg = self.calculate_team_average("Teleop_High_Goal")
        tele_miss_avg = self.calculate_team_average("Teleop_Misses")
        fouls_avg = self.calculate_team_average("Fouls")
        climb_time_avg = self.calculate_team_average("Climb_Time")

        self.log.info("Calculating medians")
        auto_low_med = self.calculate_team_median("Auto_Low_Goal")
        auto_high_med = self.calculate_team_median("Auto_High_Goal")
        tele_low_med = self.calculate_team_median("Teleop_Low_Goal")
        tele_high_med = self.calculate_team_median("Teleop_High_Goal")
        tele_miss_med = self.calculate_team_median("Teleop_Misses")
        fouls_med = self.calculate_team_median("Fouls")
        climb_time_med = self.calculate_team_median("Climb_Time")

        self.log.info("Calculating percentages")
        shooting_zone_pct = self.calculate_team_percentages(
            ['Target_Zone?', 'Initiation_Line?', 'Near_Trench?', 'Rendezvous_point?', 'Far_Trench'],
            replacements={"Yes": 1, "No": 0})
        climb_type_pct = self.calculate_team_percentages(['Climb_Type'], one_hot_encoded=False, possible_values=['Hang', 'Park', 'No Climb'])

        oprs = self.calculate_opr("totalPoints")

        self.log.info("Adding data to SQL")
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
                climb_type_pct,
                oprs
            ]
        )
        # OPR (xOPR or ixOPR)
        # Consistency scores
        # ELO
