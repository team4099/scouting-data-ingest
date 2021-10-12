import json
import numpy
import pandas as pd
from scipy.sparse.linalg import lsmr
from sklearn.preprocessing import MultiLabelBinarizer
from sqlalchemy import Column, Integer, String, Text, ForeignKey, Float, null

from SQLObjects import Base
from terminal import logger


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
        team_data = self.data_accessor.get_all_team_data_df().loc[["id", col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(
                team_data.groupby("team_id").mean(),
                how="outer",
                left_on="id",
                right_index=True,
            )
        else:
            team_data_average = self.team_list.merge(
                team_data, how="outer", left_on="team_id", right_index=True
            ).drop(columns=["id"])
        team_data_average = team_data_average.rename(columns={col: col + "_avg"})
        return team_data_average.set_index("team_id")

    def calculate_team_median(self, col):
        """

        Calculates an median by team for a metric.

        :param col: A metric column from TeamData.
        :type col: str
        :return: A Dataframe of medians
        :rtype: pandas.DataFrame
        """
        team_data = self.data_accessor.get_all_team_data_df()[["team_id", col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(
                team_data.groupby("team_id").median(),
                how="outer",
                left_on="id",
                right_index=True,
            )
        else:
            team_data_average = self.team_list.merge(
                team_data, how="outer", left_on="team_id", right_index=True
            ).drop(columns=["id"])
        team_data_average = team_data_average.rename(columns={col: col + "_med"})
        return team_data_average.set_index("team_id")

    def calculate_team_percentages(
        self, cols, one_hot_encoded=True, replacements=None, possible_values=None
    ):
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
        team_data = self.data_accessor.get_all_team_data_df()[[*cols, "id"]]
        if not one_hot_encoded:
            team_data_encoded = pd.get_dummies(team_data[cols])
            team_data[team_data_encoded.columns] = team_data_encoded[
                team_data_encoded.columns
            ]
            unused_col_names = [
                f"{col}_{name}"
                for col in cols
                for name in possible_values
                if f"{col}_{name}" not in team_data_encoded.columns
            ]
            team_data[unused_col_names] = pd.DataFrame(
                data=0, index=team_data_encoded.index, columns=unused_col_names
            )
        if replacements is not None:
            for k, v in replacements.items():
                team_data = team_data.replace(k, v)
        counts = team_data.dropna().groupby("id").mean()
        team_data_percentages = self.team_list.merge(
            counts, how="outer", left_on="id", right_index=True
        )
        team_data_percentages = team_data_percentages.rename(
            columns={c: c + "_pct" for c in cols}
        )
        return team_data_percentages.set_index("id")

    def calculate_team_percentages_quant(self, cols):
        team_data = self.data_accessor.get_all_team_data_df()[[*cols, "id"]]
        counts = team_data.dropna().groupby("id").sum()
        counts["Sum"] = counts.sum(axis=1)
        for col in cols:
            counts[f"{col}_pct"] = counts.loc[:, col] / counts["Sum"]
        counts.drop(cols, axis=1)
        return counts

    def team_data_to_sql(self, dfs):
        self.log.info("Joining Dataframes")
        full_df = dfs[0].join(dfs[1:])

        self.log.info("Adding Data")
        full_df = full_df.replace(numpy.nan, null(), regex=True)
        for row in full_df.iterrows():
            self.data_accessor.add_calculated_team_data(row[0], row[1])
        self.session.commit()

    def calculate_opr(self, metric):
        team_lists = self.data_accessor.get_teams_in_match()
        team_lists.index = team_lists.index.sortlevel(
            1, ascending=True, sort_remaining=True
        )[0]
        team_lists = (
            pd.DataFrame(team_lists)
            .reset_index()
            .rename({"Match_Key": "matchId"}, axis=1)
        )
        metric_data = self.data_accessor.get_match_data(type_df=True)[
            [
                f"score_breakdown.red.{metric}",
                f"score_breakdown.blue.{metric}",
                "matchId",
            ]
        ].sort_values(by="matchId")
        blue_data = metric_data[[f"score_breakdown.blue.{metric}", "matchId"]].rename(
            {f"score_breakdown.blue.{metric}": metric}, axis=1
        )
        red_data = metric_data[[f"score_breakdown.red.{metric}", "matchId"]].rename(
            {f"score_breakdown.red.{metric}": metric}, axis=1
        )
        blue_data["Alliance"] = "Blue"
        red_data["Alliance"] = "Red"
        merged_data = blue_data.append(red_data)

        assembled_data = pd.merge(
            team_lists, merged_data, on=["matchId", "Alliance"], how="inner"
        )[["teamid", metric]]
        mlb = MultiLabelBinarizer(sparse_output=True)
        sparse_teams = mlb.fit_transform(assembled_data["teamid"])
        oprs = lsmr(sparse_teams, assembled_data[metric].to_numpy())
        teams_with_oprs = pd.DataFrame([mlb.classes_, oprs[0]]).transpose()
        teams_with_oprs.rename({0: "teams", 1: f"{metric}_opr"}, inplace=True, axis=1)
        teams_with_oprs.set_index("teams", inplace=True)
        return teams_with_oprs

    def group_notes(self):
        team_data = self.data_accessor.get_all_team_data_df()[
            ["team_id", "match_id", "notes", "auto_notes", "teleop_notes"]
        ]
        stripped_match_id = team_data["match_id"].apply(lambda x: x[x.index("_") + 1 :])
        team_data["Comments"] = (
            "N"
            + stripped_match_id
            + ": "
            + team_data["notes"]
            + ","
            + team_data["teleop_notes"]
            + ", "
            + team_data["auto_notes"]
        )

        notes_by_teams = pd.DataFrame(
            index=team_data["team_id"].unique(), columns=["comments"]
        )

        for team in team_data["teamid"].unique():
            team_specific_notes = team_data.loc[team_data["teamid"] == team]
            notes_by_teams.at[team, "comments"] = team_specific_notes[
                "Comments"
            ].str.join("")

        return notes_by_teams

    def calculate_team_data(self):
        """
        Calculates Team Data
        """
        self.log.info("Getting a team list")
        self.team_list = self.data_accessor.get_all_teams_df()

        self.log.info("Calculating averages")
        auto_low_avg = self.calculate_team_average("auto_low_goal")
        auto_high_avg = self.calculate_team_average("auto_high_hoal")
        auto_miss_avg = self.calculate_team_average("auto_misses")
        tele_miss_avg = self.calculate_team_average("teleop_misses")
        tele_low_avg = self.calculate_team_average("teleop_low_goal")
        tele_high_avg = self.calculate_team_average("teleop_high_goal")
        tele_miss_avg = self.calculate_team_average("teleop_misses")
        fouls_avg = self.calculate_team_average("fouls")
        climb_time_avg = self.calculate_team_average("climb_time")

        self.log.info("Calculating medians")
        auto_low_med = self.calculate_team_median("auto_low_goal")
        auto_high_med = self.calculate_team_median("auto_high_goal")
        tele_low_med = self.calculate_team_median("teleop_low_goal")
        tele_high_med = self.calculate_team_median("teleop_high_goal")
        tele_miss_med = self.calculate_team_median("teleop_misses")
        fouls_med = self.calculate_team_median("fouls")
        climb_time_med = self.calculate_team_median("climb_time")

        self.log.info("Calculating percentages")
        shooting_zone_pct = self.calculate_team_percentages(
            [
                "Target_Zone?",
                "Initiation_Line?",
                "Near_Trench?",
                "Rendezvous_point?",
                "Far_Trench",
            ],
            replacements={"Yes": 1, "No": 0},
        )
        climb_type_pct = self.calculate_team_percentages(
            ["Climb_Type"],
            one_hot_encoded=False,
            possible_values=["Hang", "Park", "No_Climb"],
        )
        shoot_pct = self.calculate_team_percentages_quant(
            ["Teleop_High_Goal", "Teleop_Low_Goal", "Teleop_Misses"]
        )

        comments = self.group_notes()

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
                shoot_pct,
                oprs,
                comments,
            ]
        )
        # Consistency scores
        # ELO
