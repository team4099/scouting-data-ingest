import json
import numpy
import pandas as pd
from scipy.sparse.linalg import lsmr
from sklearn.preprocessing import MultiLabelBinarizer
from sqlalchemy import Column, Integer, String, Text, ForeignKey, Float, null

from SQLObjects import Base, ClimbType
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
        team_data = self.data_accessor.get_all_team_data_df()[["team_id", col]]
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
        return team_data_average.set_index("id")

    def calculate_team_average_filter(self, col, filter_col):
        """

        Calculates an average by team for a metric.

        :param col: A metric column from TeamData.
        :type col: str
        :return: A Dataframe of averages
        :rtype: pandas.DataFrame
        """
        team_data = self.data_accessor.get_all_team_data_df()[["team_id", col, filter_col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(
                team_data[team_data[filter_col]].groupby("team_id").mean(),
                how="outer",
                left_on="id",
                right_index=True,
            ).drop(columns=[filter_col])
        else:
            team_data_average = self.team_list.merge(
                team_data, how="outer", left_on="team_id", right_index=True
            ).drop(columns=["id"])
        team_data_average = team_data_average.rename(columns={col: col + "_avg"})
        return team_data_average.set_index("id")



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
        return team_data_average.set_index("id")
    
    def calculate_team_median_filter(self, col, filter_col):
        """

        Calculates an median by team for a metric.

        :param col: A metric column from TeamData.
        :type col: str
        :return: A Dataframe of medians
        :rtype: pandas.DataFrame
        """
        team_data = self.data_accessor.get_all_team_data_df()[["team_id", col, filter_col]].dropna()
        if (len(team_data.index)) > 0:
            team_data_average = self.team_list.merge(
                team_data[team_data[filter_col]].groupby("team_id").median(),
                how="outer",
                left_on="id",
                right_index=True,
            ).drop(columns=[filter_col])
        else:
            team_data_average = self.team_list.merge(
                team_data, how="outer", left_on="team_id", right_index=True
            ).drop(columns=["id",filter_col])
        team_data_average = team_data_average.rename(columns={col: col + "_med"})
        return team_data_average.set_index("id")

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
        team_data = self.data_accessor.get_all_team_data_df()[[*cols, "team_id"]]
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
        counts = team_data.dropna().groupby("team_id").mean()
        team_data_percentages = self.team_list.merge(
            counts, how="outer", left_on="id", right_index=True
        )
        team_data_percentages = team_data_percentages.rename(
            columns={c: c + "_pct" for c in cols}
        )
        return team_data_percentages.set_index("id")

    def calculate_team_percentages_quant(self, cols):
        team_data = self.data_accessor.get_all_team_data_df()[[*cols, "team_id"]]
        counts = team_data.dropna().groupby("team_id").sum()
        counts["Sum"] = counts.sum(axis=1)
        for col in cols:
            counts[f"{col}_pct"] = counts.loc[:, col] / counts["Sum"]
        cols.append("Sum")
        counts.drop(cols, axis=1, inplace=True)
        return counts

    def team_data_to_sql(self, dfs, col_names):
        self.log.info("Joining Dataframes")
        full_df = dfs[0].join(dfs[1:])
        full_df.rename(col_names, inplace=True, axis=1)

        self.log.info("Adding Data")
        full_df = full_df.replace(numpy.nan, null(), regex=True)
        for team, calculated_team_datum in full_df.to_dict(orient="index").items():
            self.data_accessor.add_calculated_team_datum(team, calculated_team_datum)
        self.session.commit()

    def calculate_opr(self, metric):
        #self.session.flush()
        all_matches = self.data_accessor.get_all_match_objects(
            [f"r_{metric}",
             f"b_{metric}"]
        ) #getting all data 

        all_alliances = self.data_accessor.get_all_alliance()
        
        alliance_metric_info = []
        for match in range(int(len(all_alliances)/2)):
            alliance_metric_info.append([all_alliances[match], all_matches[match][1]])
        for match in range(int(len(all_alliances)/2)):
            alliance_metric_info.append([all_alliances[int(len(all_alliances)/2) + match], all_matches[match][2]])
        
        assembled_data = pd.DataFrame(numpy.array(alliance_metric_info), columns=['teams', 'metricdata'])
        mlb = MultiLabelBinarizer(sparse_output=True)
        sparse_teams = mlb.fit_transform(assembled_data["teams"])
        oprs = lsmr(sparse_teams, assembled_data['metricdata'].apply(lambda val: int(val)).to_numpy())
        teams_with_oprs = pd.DataFrame([mlb.classes_, oprs[0]]).transpose()
        teams_with_oprs.rename({0: "teams", 1: f"{metric}_opr"}, inplace=True, axis=1)
        teams_with_oprs.set_index("teams", inplace=True)
        
        return teams_with_oprs

    def group_notes(self):
        team_data = self.data_accessor.get_all_team_data_df()[
            ["team_id", "match_id", "notes", "auto_notes", "teleop_notes"]
        ]
        stripped_match_id = team_data["match_id"].apply(lambda x: x[x.index("_") + 1 :])
        team_data["comments"] = (
            "N"
            + stripped_match_id
            + ": "
            + team_data["notes"].astype(str)
            + ","
            + team_data["teleop_notes"].astype(str)
            + ", "
            + team_data["auto_notes"].astype(str)
        )

        notes_by_teams = pd.DataFrame(
            index=team_data["team_id"].unique(), columns=["comments"]
        )

        for team in team_data["team_id"].unique():
            team_specific_notes = team_data.loc[team_data["team_id"] == team]
            notes_by_teams.at[team, "comments"] = "".join(
                team_specific_notes["comments"]
            )

        return notes_by_teams

    def calculate_team_data(self):
        """
        Calculates Team Data
        """
        self.log.info("Getting a team list")
        self.team_list = self.data_accessor.get_all_teams_df()

        if len(self.data_accessor.get_all_team_data_df().index) == 0:
            return

        self.log.info("Calculating averages")
        auto_lower_avg = self.calculate_team_average("auto_lower_hub")
        auto_upper_avg = self.calculate_team_average("auto_upper_hub")
        auto_miss_avg = self.calculate_team_average("auto_misses")
        tele_lower_avg = self.calculate_team_average("teleop_lower_hub")
        tele_upper_avg = self.calculate_team_average("teleop_upper_hub")
        tele_miss_avg = self.calculate_team_average("teleop_misses")
        low_climb_time_avg = self.calculate_team_average_filter("low_rung_climb_time","attempted_low")
        mid_climb_time_avg = self.calculate_team_average_filter("mid_rung_climb_time","attempted_mid")
        high_climb_time_avg = self.calculate_team_average_filter("high_rung_climb_time", "attempted_high")
        traversal_climb_time_avg = self.calculate_team_average_filter("traversal_rung_climb_time", "attempted_traversal")

        self.log.info("Calculating medians")
        auto_lower_med = self.calculate_team_median("auto_lower_hub")
        auto_upper_med = self.calculate_team_median("auto_upper_hub")
        auto_miss_med = self.calculate_team_median("auto_misses")
        tele_lower_med = self.calculate_team_median("teleop_lower_hub")
        tele_upper_med = self.calculate_team_median("teleop_upper_hub")
        tele_miss_med = self.calculate_team_median("teleop_misses")
        low_climb_time_med = self.calculate_team_median_filter("low_rung_climb_time", 'attempted_low')
        mid_climb_time_med = self.calculate_team_median_filter("mid_rung_climb_time", "attempted_mid")
        high_climb_time_med = self.calculate_team_median_filter("high_rung_climb_time", "attempted_high")
        traversal_climb_time_med = self.calculate_team_median_filter("traversal_rung_climb_time", "attempted_traversal")

        self.log.info("Calculating OPR")
        total_points_opr= self.calculate_opr("total_points")
        # everything opr but climb
        # points scored during auto
        # points scored during teleop
        # run a sim for 2020 vahay and check reliability of 

        self.log.info("Calculating percentages")
        shooting_zone_pct = self.calculate_team_percentages(
            [
                "from_fender",
                "from_elsewhere_in_tarmac",
                "from_launchpad",
                "from_terminal",
                "from_hangar_zone",
                "from_elsewhere_on_field"
            ],
            replacements={True: 1, False: 0},
        )
        auto_shooting_zone_pct = self.calculate_team_percentages(
            [
                "auto_from_fender",
                "auto_from_elsewhere_in_tarmac",
                "auto_from_launchpad",
                "auto_from_terminal",
                "auto_from_hangar_zone",
                "auto_from_elsewhere_on_field"
            ],
            replacements={True: 1, False: 0},
        )
        attempted_climbs_pct = self.calculate_team_percentages(
            [
                "attempted_low",
                "attempted_mid",
                "attempted_high",
                "attempted_traversal"
            ],
            replacements={True: 1, False: 0},
        )
        climb_type_pct = self.calculate_team_percentages(
            ["final_climb_type"],
            one_hot_encoded=False,
            possible_values=[ClimbType.traversal, ClimbType.high, ClimbType.mid, ClimbType.low, ClimbType.none],
        )
        shoot_pct = self.calculate_team_percentages_quant(
            ["teleop_upper_hub", "teleop_lower_hub", "teleop_misses"]
        )

        comments = self.group_notes()

        self.log.info("Adding data to SQL")
        self.team_data_to_sql(
            [
                auto_lower_avg,
                auto_upper_avg,
                auto_miss_avg,
                tele_lower_avg,
                tele_upper_avg,
                tele_miss_avg,
                low_climb_time_avg,
                mid_climb_time_avg,
                high_climb_time_avg,
                traversal_climb_time_avg,
                auto_lower_med,
                auto_upper_med,
                auto_miss_med,
                tele_lower_med,
                tele_upper_med,
                tele_miss_med,
                low_climb_time_med,
                mid_climb_time_med,
                high_climb_time_med,
                traversal_climb_time_med,
                shooting_zone_pct,
                auto_shooting_zone_pct,
                attempted_climbs_pct,
                climb_type_pct,
                shoot_pct,
                comments,
            ],
            {
                "from_fender_pct": "from_fender_usage",
                "from_elsewhere_in_tarmac_pct": "from_elsewhere_in_tarmac_usage",
                "from_launchpad_pct": "from_launchpad_usage",
                "from_terminal_pct": "from_terminal_usage",
                "from_hangar_zone_pct": "from_hangar_zone_usage",
                "from_elsewhere_on_field_pct": "from_elsewhere_on_field_usage",
                "auto_from_fender_pct": "auto_from_fender_usage",
                "auto_from_elsewhere_in_tarmac_pct": "auto_from_elsewhere_in_tarmac_usage",
                "auto_from_launchpad_pct": "auto_from_launchpad_usage",
                "auto_from_terminal_pct": "auto_from_terminal_usage",
                "auto_from_hangar_zone_pct": "auto_from_hangar_zone_usage",
                "auto_from_elsewhere_on_field_pct": "auto_from_elsewhere_on_field_usage",
                "attempted_low_pct": "attempted_low_usage",
                "attempted_mid_pct":"attempted_mid_usage",
                "attempted_high_pct": "attempted_high_usage",
                "attempted_traversal_pct": "attempted_traversal_usage",
                "final_climb_type_ClimbType.traversal": "traversal_rung_pct",
                "final_climb_type_ClimbType.high": "high_rung_pct",
                "final_climb_type_ClimbType.mid": "mid_rung_pct",
                "final_climb_type_ClimbType.low": "low_rung_pct",
                "final_climb_type_ClimbType.none": "none_pct",
                "teleop_high_goal_pct": "teleop_high_pct",
                "teleop_low_goal_pct": "teleop_low_pct",
                "teleop_misses_pct": "teleop_miss_pct",
            },
        )
        # Consistency scores
        # ELO
