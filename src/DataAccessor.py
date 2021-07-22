import datetime
import pandas as pd
from sqlalchemy import exists, update
import copy

from SQLObjects import Alliance, Info, Matches, Teams, Warnings, Predictions, Scouts
from terminal import logger


class DataAccessor:
    def __init__(self, engine, session, connection, config):
        self.log = logger.opt(colors=True)

        self.log.info("Starting DataAccessor")
        # Loading configuration
        self.log.info("Loading Configuration")

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

    def get_match_data(self, match_key=None, color=None, type_df=True, occured=True):
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
        if occured:
            query = query.filter(self.MatchDataObject.actual_time != datetime.datetime.fromtimestamp(0))
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
        return data.groupby(["Match_Key", "Alliance"])["teamid"].apply(list)

    def add_match_data(self, key: str, data):
        """

        Adds a match to the database if it doesn't already exist.

        :param key: Match Key
        :type key: str
        :param data: A year-specific Match Data Object
        :type data: DataInput.MatchData2020
        :rtype: None
        """
        occurred = False
        if self.check_if_match_data_exists(key):
            if not self.check_if_match_data_exists(key, data.actual_time):
                occurred = True
                match_vars = dict(vars(data))
                match_vars.pop("_sa_instance_state")
                update_stmt = update(self.MatchDataObject).where(self.MatchDataObject.matchId == key).values(**match_vars)
                self.connection.execute(update_stmt)
                self.session.commit()
                self.process_predictions(key, data.winning_alliance)
                return
            else:
                self.log.warning(f"MatchData for match {key} already exists. It will not be added.")
                return

        m = Matches(id=key, data_list=data)

        self.session.add(m)

    def add_alliances_for_match(self, key, red_teams, blue_teams):
        for rt in red_teams:
            a = Alliance(matchid=key, teamid=rt, color="Red")
            if not self.check_if_alliance_exists(key,rt,"Red"):
                self.session.add(a)
        for bt in blue_teams:
            a = Alliance(matchid=key, teamid=bt, color="Blue")
            if not self.check_if_alliance_exists(key,bt,"Blue"):
                self.session.add(a)

    def get_alliance(self, match_key=None, alliance=None, teamid=None, type_df=True):
        query = self.session.query(Alliance)

        if match_key is not None:
            query = query.filter(Alliance.matchid == match_key)

        if alliance is not None:
            query = query.filter(Alliance.color == alliance)

        if teamid is not None:
            query = query.filter(Alliance.teamid == teamid)

        if type_df:
            return pd.read_sql_query(query.statement, self.sql_connection())
        else:
            return query[:]

    def get_calculated_team_data(self, teamid, type_df=True):
        query = self.session.query(self.CalculatedTeamDataObject).filter(self.CalculatedTeamDataObject.teamid == teamid)

        if type_df:
            return pd.read_sql_query(query.statement, self.sql_connection())
        else:
            return query[:]

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
    
    def add_scout(self, name):
        if self.check_if_scout_exists(name):
            self.log.warning("Scout already exists. They will not be added.")
            return
        s = Scouts(id=name,points=0,streak=0,active=True)
        self.session.add(s)
        self.session.commit()

    def check_if_scout_exists(self, name):
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(Scouts.id == name)
            )
        return ret
    
    def add_prediction(self, scout, match, prediction):
        if self.check_if_prediction_exists(scout, match):
            self.update_prediction(scout, match, prediction)
            return
        p = Predictions(scout=scout, match=match, prediction=prediction)
        self.session.add(p)
        self.session.commit()

    def get_prediction(self, scout=None, match=None, prediction=None, type_df=True):
        query = self.session.query(Predictions)

        if scout is not None:
            query = query.filter(Predictions.scout == scout)

        if match is not None:
            query = query.filter(Predictions.match == match)

        if prediction is not None:
            query = query.filter(Predictions.prediction == prediction)

        if type_df:
            return pd.read_sql_query(query.statement, self.sql_connection())
        else:
            return query[:]

    def check_if_prediction_exists(self, scout, match):
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(Predictions.scout == scout)
                .where(Predictions.match == match)
            )
        return ret

    def check_if_alliance_exists(self, match_key, teamid, color):
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(Alliance.matchid == match_key)
                .where(Alliance.teamid == teamid)
                .where(Alliance.color == color)
            )
        return ret

    def update_prediction(self, scout, match, prediction):
        prediction = self.session.query(Predictions).filter(Predictions.scout == scout).filter(Predictions.match == match)[0]
        prediction.prediction = prediction
        self.session.commit()

    def get_scouts(self):
        query = self.session.query(Scouts)

        data = pd.read_sql_query(query.statement, self.sql_connection())
        return data

    def update_scout(self, name, active=None, points=None, streak=None):
        query = self.session.query(Scouts)

        query = query.filter(Scouts.id == name)[0]

        if active is not None:
            query.active = active
        
        if points is not None:
            query.points = points
        
        if streak is not None:
            query.points = streak

        self.session.commit()

    def delete_scout(self, name):
        query = self.session.query(Scouts).filter(Scouts.id==name)
        query.delete()
        self.session.commit()

    def delete_match(self, match_key):
        query = self.session.query(self.MatchDataObject).filter(self.MatchDataObject.matchId == match_key)
        query.delete()
        self.session.flush()
        self.session.commit

    def process_predictions(self, match, result):
        query = self.session.query(Predictions)

        query = query.filter(Predictions.match == match)[:]
        for prediction in query:
            scout = self.session.query(Scouts).filter(Scouts.id == prediction.scout)[0]
            if prediction.prediction == result:
                scout.points += 10 + scout.streak * 0.5
                scout.streak += 1
            else:
                scout.streak = 0
        self.session.commit()
             

    def add_warning(self, category, match, alliance, content, ignore=False):
        if self.check_if_warning_exists(category, match, alliance, content):
            self.log.warning("Warning already exists. It will not be added.")
            return
        w = Warnings(category=category, match=match, alliance=alliance, content=content, ignore=ignore)
        self.session.add(w)

    def check_if_warning_exists(self, category, match, alliance, content):
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(Warnings.category == category)
                .where(Warnings.match == match)
                .where(Warnings.alliance == alliance)
                .where(Warnings.content == content)
            )
        return ret
    
    def get_warnings(self):
        query = self.session.query(Warnings)

        data = pd.read_sql_query(query.statement, self.sql_connection())
        return data

    def update_warning(self, id, ignore):
        query = self.session.query(Warnings)

        query = query.filter(Warnings.id == id)[0]

        query.ignore = ignore

        self.session.flush()
        self.session.commit()

    def delete_warnings(self):
        query = self.session.query(Warnings)
        query.delete()

    
    def add_info(self, id, value):
        i = Info(id=id, value=value)
        self.session.add(i)
        self.session.flush()


    def update_info(self, id, value):
        query = self.session.query(Info)

        query = query.filter(Info.id == id)[0]

        query.value = value

        self.session.commit()

    def get_info(self):
        query = self.session.query(Info)

        data = pd.read_sql_query(query.statement, self.sql_connection())
        return data

    def delete_calculated_team_data(self, team_id=None):
        """
        Deletes a CalculatedTeamData Object from the database.

        :param team_id: A specific Team ID to be deleted
        :type team_id: str
        :rtype: None
        """
        query = self.session.query(self.CalculatedTeamDataObject)
        if team_id is not None:
            query = query.filter(self.CalculatedTeamDataObject.teamid == team_id)

        query.delete()

    def delete_team_data(self, team_id=None, match_key=None):
        """
        Deletes a TeamData Object from the database.

        :param team_id: A specific Team ID to be deleted
        :type team_id: str
                :param team_id: A specific Team ID to be deleted
        :type team_id: str
        :rtype: None
        """
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

    def check_if_match_data_exists(self, match_key, time=None):
        """

        Checks if a given match data for a match key exists in the database.

        :param match_key: A Match Key
        :type match_key: str
        :return: Whether the match data exists or not.
        :rtype: bool
        """
        with self.session.no_autoflush:
            query = exists().where(self.MatchDataObject.matchId == match_key)
            if time is not None:
                query = query.where(self.MatchDataObject.actual_time == time)
            ((ret,),) = self.session.query(query)
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
