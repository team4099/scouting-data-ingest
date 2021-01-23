import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from terminal import console, logger
import pandas as pd
from re import search


class DataProcessor:
    def __init__(self, session, engine, connection):
        self.log = logger

        self.log.info("[bold green]Starting [bold purple]DataProcessor")
        # Loading configuration
        self.log.info("[bold purple]Loading Configuration")
        with open("config/config.json") as f:
            config = json.load(f)

        self.config = config

        # Connecting to MySQL
        self.log.info("[bold purple]Connecting to MySQL")
        self.engine = engine
        self.session = session
        self.connection = connection

        self.log.info("[bold purple]Initializing Variables")
        self.warning_dict = {}
        self.last_checked = None

        self.log.info("[bold purple]DataProcessor Loaded!")

    def checkEqualsByAlliance(
        self,
        team_data_columns,
        match_data_columns,
        team_weights=None,
        match_weights=None,
    ):  # iterable of series, series
        warnings = []
        red_association = pd.read_sql_table("red_association", self.connection)
        blue_association = pd.read_sql_table("blue_association", self.connection)

        matches = team_data_columns["Match_Key"].unique()
        for match in matches:
            for color, colored_data in match_data_columns.items():
                if len(colored_data[colored_data["matchId"] == match].index) == 0:
                    self.log.error(
                        f"TBA Data for the {color} alliance in {match} does not exist. It will be skipped"
                    )

        if team_weights is None:
            team_weights = [
                1
                for i in range(
                    len(
                        [
                            col
                            for col in team_data_columns.columns
                            if col != "Match_Key" and col != "teamid"
                        ]
                    )
                )
            ]
        if match_weights is None:
            match_weights = [
                1
                for i in range(
                    len(
                        [
                            col
                            for col in match_data_columns["Blue"].columns
                            if col != "matchId"
                        ]
                    )
                )
            ]
        sumTeamColumn = pd.Series([0 for i in range(len(team_data_columns.index))])
        for column, weight in zip(
            [
                col
                for col in team_data_columns.columns
                if col != "Match_Key" and col != "teamid"
            ],
            team_weights,
        ):
            sumTeamColumn += team_data_columns[column] * weight
        team_data_columns["Sum"] = sumTeamColumn

        for color, data in match_data_columns.items():
            sumMatchColumn = pd.Series([0 for i in range(len(data.index))])
            for column, weight in zip(
                [col for col in data.columns if col != "matchId"], match_weights
            ):
                sumMatchColumn += data[column] * weight
            data["Sum"] = sumMatchColumn
            match_data_columns[color] = data

        for color, data in match_data_columns.items():
            for index, row in data.iterrows():
                curr_match_data = team_data_columns.loc[
                    team_data_columns["Match_Key"] == row["matchId"]
                ]
                if len(curr_match_data.index) < 6:
                    # self.log.warning(f"Team Data for {row['matchId']} does not exist. It will be skipped.") #TODO: Re-enable when all matches are added
                    continue

                if color == "Red":
                    curr_alliance_data = curr_match_data[
                        curr_match_data["teamid"].isin(
                            red_association[
                                red_association["match_id"] == row["matchId"]
                            ]["team_id"]
                        )
                    ]
                elif color == "Blue":
                    curr_alliance_data = curr_match_data[
                        curr_match_data["teamid"].isin(
                            blue_association[
                                blue_association["match_id"] == row["matchId"]
                            ]["team_id"]
                        )
                    ]
                else:
                    self.log.warning(f"Color {color} is not valid")
                    return

                if row["Sum"] != curr_alliance_data["Sum"].sum():
                    warning = f'For the {color} alliance in match {row["matchId"]}, the {", ".join([col for col in team_data_columns.columns if col not in ["Match_Key", "teamid", "Sum"]])} columns do not equal the {", ".join([col for col in match_data_columns["Blue"].columns if col not in ["matchId", "teamid", "Sum"]])} columns'
                    self.log.error(warning)
                    warnings.append(warning)

        return warnings

    def checkSame(
        self, team_data_column, match_data_column, team_orders
    ):  # series, series
        warnings = []

        matches = team_data_column["Match_Key"].unique()
        for match in matches:
            for color, colored_data in match_data_column.items():
                if len(colored_data[colored_data["matchId"] == match].index) == 0:
                    self.log.warning(
                        f"TBA Data for the {color} alliance in {match} does not exist. It will be skipped."
                    )

        for color, data in match_data_column.items():
            for index, row in data.iterrows():
                curr_match_data = team_data_column.loc[
                    team_data_column["Match_Key"] == row["matchId"]
                ]
                if len(curr_match_data.index) < 6:
                    # self.log.warning(f"Team Data for {row['matchId']} does not exist. It will be skipped.") #TODO: Re-enable when all matches are added
                    continue

                if color == "Red":
                    curr_order = team_orders[team_orders["matchId"] == row["matchId"]]
                    curr_order = curr_order.loc[
                        :,
                        [
                            "alliances.red.team_keys.1",
                            "alliances.red.team_keys.2",
                            "alliances.red.team_keys.3",
                        ],
                    ]
                elif color == "Blue":
                    curr_order = team_orders[team_orders["matchId"] == row["matchId"]]
                    curr_order = curr_order.loc[
                        :,
                        [
                            "alliances.blue.team_keys.1",
                            "alliances.blue.team_keys.2",
                            "alliances.blue.team_keys.3",
                        ],
                    ]
                else:
                    self.log.error(f"Color {color} is not valid")
                    return warnings
                curr_order = curr_order.T.rename(
                    columns={curr_order.index[0]: "teamid"}
                )
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
                    for index, r in comparison.iterrows():
                        warning = f'For the {color} alliance in match {row["matchId"]}, {curr_order.loc[curr_order.index[index], "teamid"]}\'s endgame status is recorded as {r["self"]} while TBA has it as {r["other"]}'
                        self.log.error(warning)
                        warnings.append(warning)

        return warnings

    def checkKey(self, team_data_column):  # series
        warnings = []
        for index, key in team_data_column.iteritems():
            if not search(r"2020[a-z]{4,5}_(qm|sf|qf|f)\d{1,2}(m\d{1})*", key):
                warning = (
                    f"Match Key in TeamData2020 at index {index} is not a proper key"
                )
                self.log.warning(warning)
                warnings.append(warning)
        return warnings

    def checkData(self):
        warnings = {}
        self.log.info("[bold purple]Validating Data")
        self.log.info("[bold purple]Loading Data")
        try:
            team_data = pd.read_sql_table(
                f"teamdata{self.config['Year']}", self.connection
            )
        except Exception as e:
            self.log.info(
                f'Table teamdata{self.config["Year"]} not found, trying TeamData{self.config["Year"]}'
            )
            team_data = pd.read_sql_table(
                f"TeamData{self.config['Year']}", self.connection
            )
            self.log.info(f'Table TeamData{self.config["Year"]} found')

        try:
            match_data = pd.read_sql_table(
                f"matchdata{self.config['Year']}", self.connection
            )
        except Exception as e:
            self.log.info(
                f'Table matchdata{self.config["Year"]} not found, trying MatchData{self.config["Year"]}'
            )
            match_data = pd.read_sql_table(
                f"MatchData{self.config['Year']}", self.connection
            )
            self.log.info(f'Table MatchData{self.config["Year"]} found')

        self.log.info("[bold purple]Checking TeamData match keys")
        warnings["Match Key Violations"] = self.checkKey(team_data["Match_Key"])

        self.log.info("Checking for Auto Power Cell Low Goal Violations")
        warnings["Auto Power Cell Low Goal Violations"] = self.checkEqualsByAlliance(
            team_data.loc[:, ["teamid", "Match_Key", "Cells_scored_in_Low_Goal"]],
            {
                "Blue": match_data.loc[
                    :, ["matchId", "score_breakdown.blue.autoCellsBottom"]
                ],
                "Red": match_data.loc[
                    :, ["matchId", "score_breakdown.red.autoCellsBottom"]
                ],
            },
        )

        self.log.info("[bold purple]Checking for Auto Power Cell High Goal Violations")
        warnings["Auto Power Cell High Goal Violations"] = self.checkEqualsByAlliance(
            team_data.loc[:, ["teamid", "Match_Key", "Cells_scored_in_High_Goal"]],
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

        self.log.info("[bold purple]Checking for Teleop Power Cell Low Goal Violations")
        warnings["Teleop Power Cell Low Goal Violations"] = self.checkEqualsByAlliance(
            team_data.loc[:, ["teamid", "Match_Key", "Low_Goal"]],
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
            "[bold purple]Checking for Teleop Power Cell High Goal Violations"
        )
        warnings["Teleop Power Cell High Goal Violations"] = self.checkEqualsByAlliance(
            team_data.loc[:, ["teamid", "Match_Key", "High_Goal"]],
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

        self.log.info("[bold purple]Checking for Endgame Status Violations")
        warnings["Endgame Status Violations"] = self.checkSame(
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
            match_data.loc[
                :,
                [
                    "matchId",
                    "alliances.red.team_keys.1",
                    "alliances.red.team_keys.2",
                    "alliances.red.team_keys.3",
                    "alliances.blue.team_keys.1",
                    "alliances.blue.team_keys.2",
                    "alliances.blue.team_keys.3",
                ],
            ],
        )

        return warnings
