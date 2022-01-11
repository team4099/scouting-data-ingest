import re

import pandas
import pandas as pd
from loguru import logger
from SQLObjects import Alliance, ClimbType


class DataProcessor:
    """Validates Data in multiple metrics"""

    def __init__(self, data_accessor, config, err_cond=2):
        """

        :param data_accessor: An initialized DataAccessor object
        :type data_accessor: DataAccessor.DataAccessor
        :param err_cond: A tolerance value for data errors. Differences between data less than this value will be accepted.
        :type err_cond: int
        """
        self.log = logger.opt(colors=True)

        self.log.info("Starting DataProcessor")
        # Loading configuration
        self.log.info("Loading Configuration")

        self.config = config
        self.data_accessor = data_accessor

        self.log.info("Initializing Variables")
        self.warning_dict = {}
        self.last_checked = None
        self.errors = []
        self.error_condition = err_cond
        self.team_data = None
        self.match_data = None
        self.alliance_data = None
        self.clean_tags = re.compile("<.*?>")

        self.log.info("DataProcessor Loaded!")

    @staticmethod
    def get(obj, metric, default=None):
        res = getattr(obj,metric, default)
        if default is not None:
            if res is None:
                return default
        return res

    def check_equals_by_alliance(self, category, team_metrics, match_metrics, team_weights=None, match_weights=None):
        if team_weights is None:
            team_weights = [1]*len(team_metrics)
        if match_weights is None:
            match_weights = [1]*len(match_metrics)
        for match, alliances in zip(self.match_data, self.alliance_data):
            alliance_r_sum = sum([sum([DataProcessor.get(team, metric,0)*weight for metric, weight in zip(team_metrics, team_weights)]) for team in alliances[0]])
            alliance_b_sum = sum([sum([DataProcessor.get(team, metric,0)*weight for metric, weight in zip(team_metrics, team_weights)]) for team in alliances[1]])

            match_r_sum = sum([DataProcessor.get(match,f"r_{metric}",0)*weight for metric,weight in zip(match_metrics, match_weights)])
            match_b_sum = sum([DataProcessor.get(match,f"b_{metric}",0)*weight for metric,weight in zip(match_metrics, match_weights)])

            for alliance_sum, match_sum, color in zip([alliance_r_sum,alliance_b_sum],[match_r_sum,match_b_sum],["red","blue"]):
                if abs(alliance_sum - match_sum) > self.error_condition:
                    self.errors.append(alliance_sum - match_sum)
                    warning_desc = f'<b>{match.match_id}{" " if len(match.match_id) < 14 else ""}</b> - <{color}>{color}</> - '
                    team_col_names = ", ".join(team_metrics)
                    match_col_names = ", ".join(match_metrics)
                    warning = f'Sum of the {team_col_names} columns (<d><green>{alliance_sum}</></>) does not equal the sum of the TBA columns {match_col_names} (<d><green>{match_sum}</></>)'
                    self.log.log("DATA", warning_desc + warning)
                    self.data_accessor.add_warning(match.match_id,Alliance(color),category,re.sub(self.clean_tags, '', warning))
        self.data_accessor.session.flush()

    def check_same(self, category, team_metric, match_metrics, team_default=None, tba_default=None):
        if team_default is None:
            team_default = ""
        if tba_default is None:
            tba_default = ""
        for match, alliances in zip(self.match_data, self.alliance_data):
            for alliance, color in zip(alliances, ["red", "blue"]):
                for team, metric in zip(alliance, match_metrics):
                    team_val = DataProcessor.get(team, team_metric, team_default)
                    tba_val = DataProcessor.get(match, f"{color[0]}_{metric}", tba_default)
                    if team_val != tba_val:
                        warning_desc = f'<b>{match.match_id}{" " if len(match.match_id) < 14 else ""}</b> - <{color}>{color}</> - '
                        warning = f'{team.team.id}\'s endgame status is recorded as <d><blue>{team_val.value}</></> while TBA has it as <d><blue>{tba_val.value}</></>'
                        self.log.log("DATA", warning_desc + warning)
                        self.data_accessor.add_warning(match.match_id,Alliance(color),category,re.sub(self.clean_tags, '', warning))
        self.data_accessor.session.flush()


    def check_key(self, category, key_name):
        for team_datum in self.team_data:
            if not re.search(r"2020[a-z]{4,5}_(qm|sf|qf|f)\d{1,2}(m\d{1})*", DataProcessor.get(team_datum, key_name,"")):
                warning = (
                    f"Match Key in TeamData with id {team_datum.id} is not a proper key"
                )
                self.log.warning(warning)
                self.data_accessor.add_warning(category=category,content=re.sub(self.clean_tags, '', warning))


    def check_data(self):
        """

        Run the user defined checks on the data

        :return: A dictionary of warnings
        :rtype: Dict[str, List]
        """
        self.log.info("Validating Data")
        self.log.info("Loading Data")

        self.team_data = self.data_accessor.get_team_data()
        self.match_data = self.data_accessor.get_match_datum()
        self.alliance_data = [[[None, None, None] for j in range(2)] for i in range(len(self.match_data))]

        for team_datum in self.team_data:
            self.alliance_data[team_datum.match.match_number - 1][0 if team_datum.alliance == Alliance.red else 1][team_datum.driver_station - 1] = team_datum


        self.log.info("Checking TeamData match keys")
        self.check_key("Match Key Violations", "match_id")

        self.log.info("Checking for Auto Power Cell Low Goal Violations")
        self.check_equals_by_alliance("Auto Power Cell Low Goal Violations", ["auto_low_goal"],['auto_cells_bottom'])

        self.log.info("Checking for Auto Power Cell High Goal Violations")
        self.check_equals_by_alliance("Auto Power Cell High Goal Violations", ["auto_high_goal"], ['auto_cells_outer','auto_cells_inner'])

        self.log.info("Checking for Teleop Power Cell Low Goal Violations")
        self.check_equals_by_alliance("Teleop Power Cell Low Goal Violations", ["teleop_low_goal"],['teleop_cells_bottom'])

        self.log.info(
            "Checking for Teleop Power Cell High Goal Violations"
        )
        self.check_equals_by_alliance("Teleop Power Cell High Goal Violations", ["teleop_high_goal"],['teleop_cells_outer','teleop_cells_inner'])

        self.log.info("Checking for Endgame Status Violations")
        self.check_same("Endgame Status Violations", "final_climb_type", ["endgame_1", "endgame_2", "endgame_3"], team_default=ClimbType.none, tba_default=ClimbType.no_climb)
