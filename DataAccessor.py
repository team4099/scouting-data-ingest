import json
from sqlalchemy import exists
from terminal import logger
import pandas as pd
from SQLObjects import Matches, Teams


class DataAccessor:
    def __init__(self, engine, session, connection):
        self.log = logger.opt(colors=True).bind(color="cyan")

        self.log.info("Starting DataAccessor")
        # Loading configuration
        self.log.info("Loading Configuration")
        with open("config/config.json") as f:
            config = json.load(f)

        self.config = config

        # Connecting to MySQL
        self.log.info("Connecting to MySQL")
        self.engine = engine
        self.session = session
        self.connection = connection

        self.log.info("Initializing Variables")
        self.warning_dict = {}
        self.last_checked = None
        self.TeamDataObject = None
        self.MatchDataObject = None
        self.CalculatedTeamDataObject = None

        self.log.info("DataAccessor Loaded!")

    def get_teams(self):
        """

        Get all the stored teams

        :return: A Dataframe containing the Team IDs
        :rtype: pandas.DataFrame
        """
        return pd.DataFrame(pd.read_sql_query(self.session.query(Teams).statement, self.sql_connection())['id'])

    def get_team_data(self, team_id=None, match_key=None, color=None, driver_station=None, type_df: bool = True):
        """

        Gets a TeamData object with multiple optional filters

        :param team_id: A Team ID to be filtered by
        :type team_id: Union[str, None]
        :param match_key: A Match Key to be filtered by
        :type match_key: Union[str, None]
        :param color: An Alliance color to be filtered by
        :type color: Union[str, None]
        :param driver_station: A Driver Station number to be filtered by
        :type driver_station: Union[int, None]
        :param type_df: Whether this function should return a Dataframe or List
        :type type_df: bool
        :return: The requested data.
        :rtype: Union[pandas.DataFrame, List]
        """
        query = self.session.query(self.TeamDataObject)
        if team_id is not None:
            query = query.filter(self.TeamDataObject.teamid == team_id)
        if match_key is not None:
            query = query.filter(self.TeamDataObject.Match_Key == match_key)
        if color is not None:
            query = query.filter(self.TeamDataObject.Alliance == color)
        if driver_station is not None:
            query = query.filter(self.TeamDataObject.Driver_Station == driver_station)

        if type_df:
            return pd.read_sql_query(query.statement, self.sql_connection())
        else:
            return query[:]

    def get_match_data(self, match_key=None, color=None, type_df=True):
        """

        Gets a MatchData object with multiple optional filters

        :param match_key: A Match Key to be filtered by
        :type match_key: Union[str, None]
        :param color: An Alliance color to be filtered by
        :type color: Union[str, None]
        :param type_df: Whether this function should return a Dataframe or List
        :type type_df: bool
        :return: The requested data.
        :rtype: pandas.DataFrame
        """
        query = self.session.query(self.MatchDataObject)
        if match_key is not None:
            query = query.filter(self.MatchDataObject.matchId == match_key)
        if type_df:
            return pd.read_sql_query(query.statement, self.sql_connection())
        else:
            return query[:]

    def get_teams_in_match(self, match_key=None):
        query = self.session.query(self.TeamDataObject)

        if match_key is not None:
            query = query.filter(self.TeamDataObject.Match_Key.in_(match_key))

        data = pd.read_sql_query(query.statement, self.sql_connection())
        return data.groupby(["Match_Key","Alliance"])["teamid"].apply(list)

    def add_match_data(self, key: str, data):
        """

        Adds a match to the database if it doesn't already exist.

        :param key: Match Key
        :type key: str
        :param data: A year-specific Match Data Object
        :type data: DataInput.MatchData2020
        :rtype: None
        """
        if self.check_if_match_exists(key):
            self.log.warning("MatchData already exists. It will not be added.")
            return

        m = Matches(id=key, data_list=data)

        self.session.add(m)

    def add_team(self, id: str):
        """

        Adds a Team to the Database if it doesn't already exist.

        :param id: Team ID
        :type id: str
        :rtype: None
        """
        if self.check_if_team_exists(id):
            self.log.warning("Team already exists. It will not be added.")
            return

        m = Teams(id=id)

        self.session.add(m)
        self.session.commit()

    def add_team_data(self, id: str, match_key: str, data):
        """

        Adds a TeamData Object to the database if it doesn't already exist.

        :param id: Team ID
        :type id: str
        :param match_key: Match Key
        :type match_key: str
        :param data: A pandas Series of data
        :type data: pandas.Series
        :rtype: None
        """
        if self.check_if_team_data_exists(id, match_key):
            self.log.warning("Team already exists. It will not be added.")
            return

        m = self.TeamDataObject(teamid=id, **data.to_dict())

        self.session.add(m)

    def add_calculated_team_data(self, id: str, data):
        """

        Adds a CalculatedTeamData Object to the database if it doesn't already exist.

        :param id: Team ID
        :type id: str
        :param data: A pandas Series of calculated data.
        :type data: pandas.Series
        :rtype: None
        """
        if self.check_if_calculated_team_data_exists(id):
            self.log.warning("CalculatedTeamData already exists. It will not be added.")
            return

        m = self.CalculatedTeamDataObject(teamid=id, **data.to_dict())

        self.session.add(m)

    def delete_calculated_team_data(self, team_id=None):
        query = self.session.query(self.CalculatedTeamDataObject)
        if team_id is not None:
            query = query.filter(self.CalculatedTeamDataObject.teamid == team_id)

        query.delete()

    def delete_team_data(self, team_id=None, match_key=None):
        query = self.session.query(self.TeamDataObject)
        if team_id is not None:
            query = query.filter(self.TeamDataObject.teamid == team_id)
        if match_key is not None:
            query = query.filter(self.TeamDataObject.Match_Key == match_key)

        query.delete()

    def check_if_team_data_exists(self, team_id, match_key):
        """

        Checks if a given teamdata for a team and match exists in the database.

        :param team_id: Team ID
        :type team_id: str
        :param match_key: Match Key
        :type match_key: str
        :return: Whether the team data exists or not.
        :rtype: bool
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(self.TeamDataObject.teamid == team_id)
                .where(self.TeamDataObject.Match_Key == match_key)
            )
        return ret

    def check_if_calculated_team_data_exists(self, team_id):
        """

        Checks if a given calculated team data for a team exists in the database.

        :param team_id: Team ID
        :type team_id: str
        :return: Whether the calculated team data exists or not.
        :rtype: bool
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(self.CalculatedTeamDataObject.teamid == team_id)
            )
        return ret

    def check_if_match_data_exists(self, match_key):
        """

        Checks if a given match data for a match key exists in the database.

        :param match_key: A Match Key
        :type match_key: str
        :return: Whether the match data exists or not.
        :rtype: bool
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(self.MatchDataObject.matchId == match_key)
            )
        return ret

    def check_if_team_exists(self, team_id):
        """

        Checks if a given team for a team ID exists in the database.

        :param team_id: Team ID
        :type team_id: str
        :return: Whether the team exists or not.
        :rtype: bool
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(Teams.id == team_id)
            )
        return ret

    def check_if_match_exists(self, match_key):
        """

        Checks if a given match for a match key exists in the database.

        :param match_key: A Match Key
        :type match_key: str
        :return: Whether the match exists or not.
        :rtype: bool
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(Matches.id == match_key)
            )
        return ret

    def sql_connection(self):
        return self.engine.connect()
