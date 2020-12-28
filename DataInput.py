import gspread
from sqlalchemy import create_engine, Table, exists, null
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, ForeignKey, Integer, String, Boolean, Float, Text
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import text
import json
import numpy
import requests
import pandas as pd
from warnings import filterwarnings, warn
from datetime import datetime
from terminal import console

# Get rid of warnings
filterwarnings('ignore',module='sqlalchemy')

# Setting Up SQL
Base = declarative_base()

association_red_table = Table('red_association', Base.metadata,
                              Column('team_id', String(50), ForeignKey('team.id')),
                              Column('match_id', String(50), ForeignKey('match.id'))
                              )
association_blue_table = Table('blue_association', Base.metadata,
                               Column('team_id', String(50), ForeignKey('team.id')),
                               Column('match_id', String(50), ForeignKey('match.id'))
                               )


class Matches(Base):
    __tablename__ = 'match'
    id = Column(String(50), primary_key=True)
    red_teams = relationship("Teams",
                             secondary=association_red_table,
                             backref="r_matches")
    blue_teams = relationship("Teams",
                              secondary=association_blue_table,
                              backref="b_matches")
    data_list = relationship('MatchData')


class Teams(Base):
    __tablename__ = 'team'
    id = Column(String(10), primary_key=True)
    data_list = relationship('TeamData')


class TeamData(Base):
    __tablename__ = 'team_data'
    id = Column(Integer, primary_key=True)
    teamid = Column(String(50), ForeignKey('team.id'))


class MatchData(Base):
    __tablename__ = 'match_data'
    id = Column(Integer, primary_key=True)
    matchId = Column(String(50), ForeignKey('match.id'))


# Main Input Object that will handle all the input
class DataInput:
    def __init__(self):
        with console.status("[bold green]Starting up...") as status:
            # Loading configuration
            console.log("Loading Configuration")
            with open('config/config.json') as f:
                config = json.load(f)

            self.config = config

            # Connecting to MySQL
            console.log("Connecting to MySQL")
            self.engine = create_engine(
                f'mysql+pymysql://{self.config["Database User"]}:{self.config["Database Password"]}@localhost/scouting')
            self.Sessiontemplate = sessionmaker()
            self.Sessiontemplate.configure(bind=self.engine)
            self.session = self.Sessiontemplate()
            self.connection = self.engine.connect()

            # Erasing old data to ensure proper column set up
            console.log("Erasing Data")
            tables = ["blue_association", "match_data", f"matchdata{self.config['Year']}", "red_association", "`match`", "team_data",
                      f"teamdata{self.config['Year']}", "team", ]
            for t in tables:
                tex = text(f"drop table if exists {t}")
                self.connection.execute(tex)
            self.session.commit()

            # Exists to use a year specific object types
            console.log("Initializing Variables")
            self.TeamDataObject = TeamData
            self.MatchDataObject = MatchData

            # Set as early as possible to make sure the first TBA response on load will provide data
            self.tbaLastModified = 'Wed, 1 Jan 100 00:00:01 GMT'
            self.sheetLastModified = None

            # Object to represent worksheet
            self.sheet = None

            # Reads config files and sets up variables and SQL from them
            self.parseConfig()

            # Creates everything and puts into SQL
            console.log("Creating ORM Objects")
            Base.metadata.create_all(self.engine)

            self.session.commit()
            console.log("Finished")

    def addMatch(self, id: str, red_teams_num: list, blue_teams_num: list, data):
        """
            Adds a match to the database with all relationships. Will prevent a match from being added if it already exists.

            :param id: The match key
            :param red_teams_num: A list containing the red alliance's team keys ex. frc4099
            :param blue_teams_num: A list containing the red alliance's team keys ex. frc4099
            :param data: A year specific MatchData object for the match

            :returns: None
        """
        if self.checkIfExists(Matches, id):
            warn('Match already exists. It will not be added.')
            return
        if len(red_teams_num) > 3 or len(blue_teams_num) > 3:
            warn('Team list cannot be bigger than 3')
            return
        red_teams = [
            Teams(id=id) if not self.checkIfExists(Teams, id) else self.session.query(Teams).filter_by(id=id)[0]
            for id in red_teams_num]
        blue_teams = [
            Teams(id=id) if not self.checkIfExists(Teams, id) else self.session.query(Teams).filter_by(id=id)[0]
            for id in blue_teams_num]

        m = Matches(id=id, red_teams=red_teams, blue_teams=blue_teams, data_list=data)

        self.session.add(m)
        self.session.commit()

    def getTeam(self, id):
        """
            Returns a team if one with the given id exists, else None.

            :param id: The team key

            :returns: Team Object
        """
        if self.checkIfExists(Teams, id):
            return self.session.query(Teams).filter_by(id=id)[0]
        else:
            warn('Team does not exist')

    def checkIfTeamDataExists(self, team_id, match_key):
        """
            Checks if a TeamData object exists.

            :param team_id: The team key
            :param match_key: The match key

            :returns: Boolean
        """
        with self.session.no_autoflush:
            (ret,), = self.session.query(
                exists().where(self.TeamDataObject.teamid == team_id).where(self.TeamDataObject.Match_Key == match_key))
        return ret

    def getTeamData(self, team_id, match_key):
        """
            Gets a team data object if it exists in the database. If it does not, it will return None and print a warning.

            :param team_id: The team id of the wanted team
            :param id: The match key

            :returns: Team Data Object
        """
        if self.checkIfTeamDataExists(team_id, match_key):
            return self.session.query(self.TeamDataObject).filter(self.TeamDataObject.teamid == team_id,
                                                                  self.TeamDataObject.Match_Key == match_key)[0]
        else:
            warn('Team Data does not exist')

    def getMatch(self, id):
        """
            Gets a match if it exists in the database. If it does not, it will return None and print a warning.

            :param id: The match key

            :returns: Match
        """
        if self.checkIfExists(Matches, id):
            return self.session.query(Matches).filter_by(id=id)[0]
        else:
            warn('Match does not exist')

    def checkIfExists(self, obj, id):
        """
            Checks if an object exists in the database.

            :param obj: Object type wanted
            :param id: id of object

            :returns: Boolean
        """
        (ret,), = self.session.query(exists().where(obj.id == id))
        return ret

    def getTBAData(self, event: str):
        headers = {'X-TBA-Auth-Key': self.config['TBA-Key'], 'If-Modified-Since': self.tbaLastModified}
        r = requests.get(f'https://www.thebluealliance.com/api/v3/event/{event}/matches', headers=headers)
        if r.status_code != 200:
            return r.status_code
        self.tbaLastModified = r.headers['Last-Modified']
        data = pd.json_normalize(r.json())
        drop_list = ['videos', 'score_breakdown']
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                pass
        data = data.infer_objects()
        for row in data.iterrows():
            x = row[1]
            for d in ['alliances.blue.dq_team_keys', 'alliances.blue.team_keys', 'alliances.blue.surrogate_team_keys',
                      'alliances.red.dq_team_keys', 'alliances.red.team_keys', 'alliances.red.surrogate_team_keys']:
                try:
                    x = x.drop(labels=[d])
                except KeyError:
                    pass
            self.addMatch(x['key'], row[1]['alliances.red.team_keys'], row[1]['alliances.blue.team_keys'],
                          self.MatchDataObject(**x.to_dict()))

        return r.status_code

    def getSheetData(self,eventName):
        data = pd.DataFrame(self.sheet.get_all_records())
        data = data.replace(r'^\s*$', numpy.nan, regex=True)
        data.astype(data.dropna().infer_objects().dtypes)
        data = data.replace(numpy.nan, null(), regex=True)
        data.columns = [i.replace(" ", "_") for i in data.columns]
        if self.sheetLastModified is None:
            pass
        elif datetime.strptime(data.iloc[-1:]['Timestamp'][0], '%m/%d/%Y %H:%M:%S') > self.sheetLastModified:
            return
        for row in data.iterrows():
            x = row[1]
            for d in ['Team_Number']:
                try:
                    x = x.drop(labels=[d])
                except KeyError:
                    pass
            x['Match_Key'] = eventName + '_' + x['Match_Key']
            if not self.checkIfTeamDataExists(f'frc{row[1]["Team_Number"]}', x['Match_Key']):
                t = self.TeamDataObject(teamid=f'frc{row[1]["Team_Number"]}', **x.to_dict())
                self.session.add(t)
            else:
                warn("This TeamData already exists. It will not be added.")
        self.session.commit()
        self.sheetLastModified = datetime.strptime(data.iloc[-1:]['Timestamp'].iloc[0], '%m/%d/%Y %H:%M:%S')

    def parseConfig(self):

        headers = {'X-TBA-Auth-Key': self.config['TBA-Key'], 'If-Modified-Since': self.tbaLastModified}
        console.log("Getting TBA Data")
        r = requests.get(f'https://www.thebluealliance.com/api/v3/event/{self.config["Year"]}vahay/matches',
                         headers=headers)
        console.log("Cleaning and Preparing data")
        data = pd.json_normalize(r.json())
        drop_list = ['videos', 'score_breakdown', 'alliances.blue.dq_team_keys', 'alliances.blue.team_keys',
                     'alliances.blue.surrogate_team_keys', 'alliances.red.dq_team_keys', 'alliances.red.team_keys',
                     'alliances.red.surrogate_team_keys']
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                pass
        data = data.infer_objects()
        console.log("Constructing Configuration")
        matchDataConfig = {}
        for col, dtype in zip(data.columns, data.dtypes):
            if dtype == numpy.float64:
                matchDataConfig[col] = f'Column(Float)'
            elif dtype == numpy.int64:
                matchDataConfig[col] = f'Column(Integer)'
            elif dtype == numpy.object:
                matchDataConfig[col] = f'Column(String(100))'
            elif dtype == numpy.bool:
                matchDataConfig[col] = f'Column(Boolean())'
            else:
                warn(f'{dtype} is not a configured datatype. It will not be used.')
        console.log("Getting sheet data")
        gc = gspread.service_account(f'./config/{self.config["Google-Credentials"]}')
        self.sheet = gc.open(f'{self.config["Spreadsheet"]}').get_worksheet(0)
        console.log("Cleaning and Preparing Data")
        data = pd.DataFrame(self.sheet.get_all_records())
        drop_list = ["Team Number"]
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                pass
        data.columns = [i.replace(" ", "_") for i in data.columns]
        data = data.replace(r'^\s*$', numpy.nan , regex=True)
        data.astype(data.dropna().infer_objects().dtypes)
        console.log("Constructing Configuration")
        teamDataConfig = {}
        for col, dtype in zip(data.columns, data.dtypes):
            if dtype == numpy.int64 or dtype == numpy.float64:
                teamDataConfig[col] = f'Column(Float())'
            elif dtype == numpy.object:
                teamDataConfig[col] = f'Column(Text(500))'
            elif dtype == numpy.bool:
                teamDataConfig[col] = f'Column(Boolean())'
            else:
                warn(f'{dtype} is not a configured datatype. It will not be used.')
        SQLConfig = {
            "TeamDataConfig": {
                "Year": self.config["Year"],
                "Attributes": teamDataConfig
            },
            "MatchDataConfig": {
                "Year": self.config["Year"],
                "Attributes": matchDataConfig
            }
        }
        console.log("Configuring SQL")
        self.parseSQLConfig(SQLConfig)

    def parseSQLConfig(self, SQLconfig):
        console.log("Constructing TeamData Object")
        t_data = {"__tablename__": f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}',
                  "__table_args__": {'extend_existing': True},
                  "id": Column(Integer, primary_key=True),
                  "teamid": Column(String(50), ForeignKey('team.id'))}

        SQLconfig['TeamDataConfig']['Attributes'] = {k: eval(v) for k, v in
                                                     SQLconfig['TeamDataConfig']['Attributes'].items()}
        self.TeamDataObject = type(f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}', (Base,),
                                   {**SQLconfig['TeamDataConfig']['Attributes'], **t_data})
        Teams.data_list = relationship(f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}')

        console.log("Constructing MatchData Object")
        m_data = {"__tablename__": f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}',
                  "__table_args__": {'extend_existing': True},
                  "id": Column(Integer, primary_key=True),
                  "matchId": Column(String(50), ForeignKey('match.id'))}

        SQLconfig['MatchDataConfig']['Attributes'] = {k: eval(v) for k, v in
                                                      SQLconfig['MatchDataConfig']['Attributes'].items()}
        self.MatchDataObject = type(f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}', (Base,),
                                    {**SQLconfig['MatchDataConfig']['Attributes'], **m_data})
        Matches.data_list = relationship(f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}', uselist=False)


d = DataInput()
d.getTBAData('2020vahay')
d.getSheetData('2020vahay')
