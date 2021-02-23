import json
from re import search
from typing import Dict, List

import pandas
import pandas as pd
from loguru import logger


class DataProcessor:
    """Validates Data in multiple metrics"""

    def __init__(self, data_accessor, err_cond=2):
        """

        :param data_accessor: An initialized DataAccessor object
        :type data_accessor: DataAccessor.DataAccessor
        :param err_cond: A tolerance value for data errors. Differences between data less than this value will be accepted.
        :type err_cond: int
        """
        self.log = logger.opt(colors=True).bind(color="light-yellow")

        self.log.info("Starting DataProcessor")
        # Loading configuration
        self.log.info("Loading Configuration")
        with open("config/config.json") as f:
            config = json.load(f)

        self.config = config
        self.data_accessor = data_accessor

        self.log.info("Initializing Variables")
        self.warning_dict = {}
        self.last_checked = None
        self.errors = []
        self.error_condition = err_cond

        self.log.info("DataProcessor Loaded!")

    def check_equals_by_alliance(self, team_columns, match_columns, team_weights=None, match_weights=None):
        """ Checks if the sum of a metric across an Alliance is equal to a metric for that Alliance in TBA.

        Given a weights list, this function will apply it to the metrics.

        :param team_columns: A Dataframe containing Team IDs, Match Keys and the metric(s) to be validated.
        :type team_columns: pandas.core.frame.DataFrame
        :param match_columns: A dictionary of dataframes for each color containing the Match Keys and metric(s) to be analyzed.
        :type match_columns: Dict[str, pandas.core.frame.DataFrame]
        :param team_weights: A list of weights for the metric columns.
        :type team_weights: None
        :param match_weights: A list of weights for the metric columns.
        :type match_weights: None
        :return: A list of warnings
        :rtype: List[str]
        """
        warnings = []
        team_metrics = [col for col in team_columns.columns if col != "Match_Key" and col != "teamid"]
        match_metrics = [[col for col in match_columns[color].columns if col != "matchId"] for color in match_columns.keys()]

        # Check if there are any matches in TeamData that aren't in MatchData and warn us
        team_matches = set(team_columns["Match_Key"].unique())
        tba_matches = set(match_columns['Blue']['matchId'].unique())
        for match in team_matches - tba_matches:
            self.log.error(f"TBA Data for {match} does not exist. It will be skipped")

        # If team weights are specified, multiply their metrics by them
        if team_weights is not None:
            for column, weight in zip(team_metrics, team_weights):
                team_columns[column] *= weight
        if match_weights is not None:
            for column, weight in zip(match_metrics, match_weights):
                match_columns[column] *= weight

        # Sum the metric columns into a column called Sum
        team_columns["Sum"] = team_columns[team_metrics].sum(axis=1)

        for (color, data), metrics in zip(match_columns.items(),match_metrics):
            data["Sum"] = data[metrics].sum(axis=1)

        # Check the Data by Alliance
        for color, data in match_columns.items():
            # Get the alliance pairings for each color
            alliance_data = self.data_accessor.get_team_data(color=color)[["teamid", "Match_Key"]]
            for index, row in data.iterrows():
                curr_match_data = team_columns.loc[team_columns["Match_Key"] == row["matchId"]]
                if len(curr_match_data.index) < 6:
                    # self.log.warning(f"Team Data for {row['matchId']} does not exist. It will be skipped.")
                    # TODO: Re-enable when all matches are added
                    continue

                # Get only the match data for the color alliance in this match
                curr_alliance_data = curr_match_data[
                    curr_match_data["teamid"].isin(
                        alliance_data[alliance_data["Match_Key"] == row["matchId"]]["teamid"]
                    )
                ]

                # Check if the sum is within an error range and log the warning
                if abs(row["Sum"] - curr_alliance_data["Sum"].sum()) > self.error_condition:
                    self.errors.append(row["Sum"] - curr_alliance_data["Sum"].sum())
                    warning_desc = f'<b>{row["matchId"]}{" " if len(row["matchId"]) < 14 else ""}</b> - {"<blue>" if color == "Blue" else "<r>"}{color}</> - '
                    col_names = ", ".join([col for col in team_columns.columns if col not in ["Match_Key", "teamid", "Sum"]])
                    warning = f'sum of the {col_names} columns <d><green>({curr_alliance_data["Sum"].sum()})</></> do not equal the sum of the TBA columns <d><green>({row["Sum"]})</></>'
                    self.log.log("DATA", warning_desc + warning)
                    warnings.append(warning_desc + warning)

        return warnings

    def check_same(self, team_column, match_column):
        """

        Check if a metric is the same for each robot in team_data and match_data using driver stations.

        :param team_column: A Dataframe containing Team IDs, Match Keys and the metric to be validated.
        :type team_column: pandas.DataFrame
        :param match_column: A dictionary of dataframes for each color containing the Match Keys and metric to be analyzed.
        :type match_column: Dict[str, pandas.DataFrame]
        :return: A list of warnings
        :rtype: List[str]
        """
        warnings = []

        # Check if there are any matches in TeamData that aren't in MatchData and warn us
        team_matches = set(team_column["Match_Key"].unique())
        tba_matches = set(match_column['Blue']['matchId'].unique())
        for match in team_matches - tba_matches:
            self.log.error(f"TBA Data for {match} does not exist. It will be skipped")


        for color, data in match_column.items():
            for index, row in data.iterrows():
                curr_match_data = team_column.loc[
                    team_column["Match_Key"] == row["matchId"]
                    ]
                if len(curr_match_data.index) < 6:
                    # self.log.warning(f"Team Data for {row['matchId']} does not exist. It will be skipped.")
                    # TODO: Re-enable when all matches are added
                    continue

                if color == "Red":
                    curr_order = self.data_accessor.get_team_data(match_key=row["matchId"], color="Red")[
                        ['Driver_Station', 'teamid']].set_index('Driver_Station')
                elif color == "Blue":
                    curr_order = self.data_accessor.get_team_data(match_key=row["matchId"], color="Blue")[
                        ['Driver_Station', 'teamid']].set_index('Driver_Station')
                else:
                    self.log.error(f"Color {color} is not valid")
                    return warnings
                curr_match_data = pd.merge(
                    curr_order, curr_match_data, on="teamid", how="inner"
                )
                curr_match_data = curr_match_data.drop(
                    labels=["teamid", "Match_Key"], axis=1
                ).iloc[:, 0]
                curr_match_data.fillna(value="None", inplace=True)

                tba_data = row.drop("matchId").reset_index(drop=True)
                comparison = curr_match_data.compare(tba_data)

                if len(comparison.index) > 0:
                    for i, r in comparison.iterrows():
                        warning_desc = f'<b>{row["matchId"]}{" " if len(row["matchId"]) < 14 else ""}</b> - {"<blue>" if color == "Blue" else "<r>"}{color}</> - '
                        warning = f'{curr_order.loc[curr_order.index[i], "teamid"]}\'s endgame status is recorded as <d><blue>{r["self"]}</></> while TBA has it as <d><blue>{r["other"]}</></>'
                        self.log.log("DATA", warning_desc + warning)
                        warnings.append(warning_desc + warning)

        return warnings

    def check_key(self, team_data_column):
        """

        Checks if any keys do not follow the appropriate format or have been entered incorrectly.

        :param team_data_column: A list of keys.
        :type team_data_column: pandas.Series
        :return: A list of warnings
        :rtype: List[str]
        """
        warnings = []
        for index, key in team_data_column.iteritems():
            if not search(r"2020[a-z]{4,5}_(qm|sf|qf|f)\d{1,2}(m\d{1})*", key):
                warning = (
                    f"Match Key in TeamData2020 at index {index} is not a proper key"
                )
                self.log.warning(warning)
                warnings.append(warning)
        return warnings

    def check_data(self):
        """

        Run the user defined checks on the data

        :return: A dictionary of warnings
        :rtype: Dict[str, List]
        """
        warnings = {}
        self.log.info("Validating Data")
        self.log.info("Loading Data")

        team_data = self.data_accessor.get_team_data()
        match_data = self.data_accessor.get_match_data()

        self.log.info("Checking TeamData match keys")
        warnings["Match Key Violations"] = self.check_key(team_data["Match_Key"])

        self.log.info("Checking for Auto Power Cell Low Goal Violations")
        warnings["Auto Power Cell Low Goal Violations"] = self.check_equals_by_alliance(
            team_data.loc[:, ["teamid", "Match_Key", "Auto_Low_Goal"]],
            {
                "Blue": match_data.loc[
                        :, ["matchId", "score_breakdown.blue.autoCellsBottom"]
                        ],
                "Red": match_data.loc[
                       :, ["matchId", "score_breakdown.red.autoCellsBottom"]
                       ],
            },
        )

        self.log.info("Checking for Auto Power Cell High Goal Violations")
        warnings["Auto Power Cell High Goal Violations"] = self.check_equals_by_alliance(
            team_data.loc[:, ["teamid", "Match_Key", "Auto_High_Goal"]],
            {
                "Blue": match_data.loc[
                        :,
                        [
                            "matchId",
                            "score_breakdown.blue.autoCellsInner",
                            "score_breakdown.blue.autoCellsOuter",
                        ],
                        ],
                "Red": match_data.loc[
                       :,
                       [
                           "matchId",
                           "score_breakdown.red.autoCellsInner",
                           "score_breakdown.red.autoCellsOuter",
                       ],
                       ],
            },
        )

        self.log.info("Checking for Teleop Power Cell Low Goal Violations")
        warnings["Teleop Power Cell Low Goal Violations"] = self.check_equals_by_alliance(
            team_data.loc[:, ["teamid", "Match_Key", "Teleop_Low_Goal"]],
            {
                "Blue": match_data.loc[
                        :, ["matchId", "score_breakdown.blue.teleopCellsBottom"]
                        ],
                "Red": match_data.loc[
                       :, ["matchId", "score_breakdown.red.teleopCellsBottom"]
                       ],
            },
        )

        self.log.info(
            "Checking for Teleop Power Cell High Goal Violations"
        )
        warnings["Teleop Power Cell High Goal Violations"] = self.check_equals_by_alliance(
            team_data.loc[:, ["teamid", "Match_Key", "Teleop_High_Goal"]],
            {
                "Blue": match_data.loc[
                        :,
                        [
                            "matchId",
                            "score_breakdown.blue.teleopCellsInner",
                            "score_breakdown.blue.teleopCellsOuter",
                        ],
                        ],
                "Red": match_data.loc[
                       :,
                       [
                           "matchId",
                           "score_breakdown.red.teleopCellsInner",
                           "score_breakdown.red.teleopCellsOuter",
                       ],
                       ],
            },
        )

        self.log.info("Checking for Endgame Status Violations")
        warnings["Endgame Status Violations"] = self.check_same(
            team_data.loc[:, ["teamid", "Match_Key", "Climb_Type"]]
                .replace(pd.NA, "Unknown")
                .replace("No Climb", "None"),
            {
                "Blue": match_data.loc[
                        :,
                        [
                            "matchId",
                            "score_breakdown.blue.endgameRobot1",
                            "score_breakdown.blue.endgameRobot2",
                            "score_breakdown.blue.endgameRobot3",
                        ],
                        ],
                "Red": match_data.loc[
                       :,
                       [
                           "matchId",
                           "score_breakdown.red.endgameRobot1",
                           "score_breakdown.red.endgameRobot2",
                           "score_breakdown.red.endgameRobot3",
                       ],
                       ],
            },
        )

        self.log.info(f"<b>{sum([len(t) for t in warnings.values()])}</b> errors were found in the data.")
        self.log.info(f"The sum errors have an average of <b>{round(sum(self.errors) / len(self.errors), 2)}</b>")
        return warnings
