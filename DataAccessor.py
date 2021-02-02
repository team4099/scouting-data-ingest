import json
from sqlalchemy import create_engine, exists
from sqlalchemy.orm import sessionmaker
from terminal import console, logger
import pandas as pd
from re import search
from SQLObjects import Matches,Teams,Base

#TODO: Fix Documentation
class DataAccessor:
    def __init__(self, engine, session, connection):
        self.log = logger

        self.log.info("[bold green]Starting [bold orange]DataAccessor")
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
        self.TeamDataObject = None
        self.MatchDataObject = None
        self.CalculatedTeamDataObject = None

        self.log.info("[bold purple]DataAccessor Loaded!")

    def getTeams(self, type_df:bool = True):
        if type_df:
            return pd.DataFrame(pd.read_sql_query(self.session.query(Teams).statement,self.connection)['id'])
        else:
            return self.session.query(Teams)[:]

    def getTeamData(self, team_id:str = None, match_key:str = None, color:str = None, driver_station:int = None, type_df:bool = True):
        """
        Gets a team data object if it exists in the database. If it does not, it will return None and print a warning.

        :param team_id: The team id of the wanted team
        :param id: The match key

        :returns: Team Data Object
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
            return pd.read_sql_query(query.statement,self.connection)
        else:
            return query[:]

    def getMatch(self, id):
        """
        Gets a match if it exists in the database. If it does not, it will return None and print a warning.

        :param id: The match key

        :returns: Match
        """
        if self.checkIfExists(Matches, id):
            return self.session.query(Matches).filter_by(id=id)[0]
        else:
            self.log.warning("Match does not exist")

    def addMatchData(self, key: str, data):
        """
        Adds a match object to the database. Will prevent a match from being added if it already exists. Will not commit changes.

        :param key: The match key
        :param data: A year specific MatchData object for the match

        :returns: None
        """
        if self.checkIfMatchExists(key):
            self.log.warning("MatchData already exists. It will not be added.")
            return

        m = Matches(id=key, data_list=data)

        self.session.add(m)

    def addTeam(self, id: str):
        """
        Adds a team object to the database. Will prevent a team_data from being added if it already exists. Will not commit changes.

        :param id: The team id

        :returns: None
        """
        if self.checkIfTeamExists(id):
            self.log.warning("Team already exists. It will not be added.")
            return

        m = Teams(id=id)

        self.session.add(m)
        self.session.commit()

    def addTeamData(self, id: str, match_key: str, data):
        """
        Adds a team_data object to the database. Will prevent a team_data from being added if it already exists. Will not commit changes.

        :param id: The team id
        :param data: A year specific TeamData object for the match

        :returns: None
        """
        if self.checkIfTeamDataExists(id,match_key):
            self.log.warning("Team already exists. It will not be added.")
            return

        m = self.TeamDataObject(teamid=id, **data.to_dict())

        self.session.add(m)

    def addCalculatedTeamData(self, id: str, data):
        """
        Adds a team_data object to the database. Will prevent a team_data from being added if it already exists. Will not commit changes.

        :param id: The team id
        :param data: A year specific TeamData object for the match

        :returns: None
        """
        #if self.checkIfCalculatedTeamDataExists(id):
        #    self.log.warning("CalculatedTeamData already exists. It will not be added.")
        #    return

        m = self.CalculatedTeamDataObject(teamid=id, **data.to_dict())

        self.session.add(m)

    def checkIfTeamDataExists(self, team_id, match_key):
        """
        Checks if a TeamData object exists.

        :param team_id: The team key
        :param match_key: The match key

        :returns: Boolean
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(self.TeamDataObject.teamid == team_id)
                .where(self.TeamDataObject.Match_Key == match_key)
            )
        return ret

    def checkIfCalculatedTeamDataExists(self, team_id):
        """
        Checks if a TeamData object exists.

        :param team_id: The team key
        :param match_key: The match key

        :returns: Boolean
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(self.CalculatedTeamDataObject.id == team_id)
            )
        return ret

    def checkIfMatchDataExists(self, match_key):
        """
        Checks if a MatchData object exists.

        :param match_key: The match key

        :returns: Boolean
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                .where(self.MatchDataObject.matchId == match_key)
            )
        return ret

    def checkIfTeamExists(self, team_id):
        """
        Checks if a Team object exists.

        :param team_id: The team key

        :returns: Boolean
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                    .where(Teams.id == team_id)
            )
        return ret

    def checkIfMatchExists(self, match_key):
        """
        Checks if a Match object exists.

        :param match_key: The match key

        :returns: Boolean
        """
        with self.session.no_autoflush:
            ((ret,),) = self.session.query(
                exists()
                    .where(Matches.id == match_key)
            )
        return ret

