from sqlalchemy import create_engine, Table, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, ForeignKey, Integer, String, Boolean, Float
from sqlalchemy.orm import relationship, sessionmaker
import json
import numpy
import requests
import pandas as pd
from time import time_ns
import warnings

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
    match = Column(String(50))
    event = Column(String(50))


class MatchData(Base):
    __tablename__ = 'match_data'
    id = Column(Integer, primary_key=True)
    matchId = Column(String(50), ForeignKey('match.id'))


class DataInput:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:robotics4099@localhost/scouting')  ## In Memory.
        self.Sessiontemplate = sessionmaker()
        self.Sessiontemplate.configure(bind=self.engine)
        self.session = self.Sessiontemplate()

        self.TeamDataObject = TeamData
        self.MatchDataObject = MatchData

        self.tbaLastModified = 'Wed, 1 Jan 100 00:00:01 GMT'

        self.config = {}
        self.parseConfig()
        self.parseSQLConfig()

        Base.metadata.create_all(self.engine)

        self.session.commit()

    def addMatch(self, id: str, red_teams_num: list, blue_teams_num: list, data):
        if self.checkIfExists(Matches, id):
            warnings.warn('Match already exists. It will not be added.')
            return
        if len(red_teams_num) > 3 or len(blue_teams_num) > 3:
            warnings.warn('Team list cannot be bigger than 3')
            return
        red_teams = [
            Teams(id=id) if not self.checkIfExists(Teams, id) else self.session.query(Teams).filter_by(id=id)[0]
            for id in red_teams_num]
        blue_teams = [
            Teams(id=id) if not self.checkIfExists(Teams, id) else self.session.query(Teams).filter_by(id=id)[0]
            for id in blue_teams_num]

        m = Matches(id=id, red_teams=red_teams, blue_teams=blue_teams,data_list=data)

        self.session.add(m)
        self.session.commit()

    def getTeam(self, id):
        if self.checkIfExists(Teams, id):
            return self.session.query(Teams).filter_by(id=id)[0]
        else:
            warnings.warn('Team does not exist')

    def getTeamData(self, id):
        if self.checkIfExists(self.TeamDataObject, id):
            return self.session.query(self.TeamDataObject).filter_by(id=id)[0]
        else:
            warnings.warn('Team Data does not exist')

    def getMatch(self, id):
        if self.checkIfExists(Matches, id):
            return self.session.query(Matches).filter_by(id=id)[0]
        else:
            warnings.warn('Match does not exist')

    def checkIfExists(self, obj, id):
        (ret,), = self.session.query(exists().where(obj.id == id))
        return ret

    def parseSQLConfig(self):
        try:

            with open('config/SQLconfig.json') as f:
                SQLconfig = json.load(f)

            t_data = {"__tablename__": f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}',
                      "__table_args__": {'extend_existing': True},
                      "id": Column(Integer, primary_key=True),
                      "teamid": Column(String(50), ForeignKey('team.id')),
                      "match": Column(String(50)),
                      "event": Column(String(50))}

            SQLconfig['TeamDataConfig']['Attributes'] = {k: eval(v) for k, v in
                                                         SQLconfig['TeamDataConfig']['Attributes'].items()}
            self.TeamDataObject = type(f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}', (Base,),
                                       {**SQLconfig['TeamDataConfig']['Attributes'], **t_data})
            Teams.data_list = relationship(f'TeamData{SQLconfig["TeamDataConfig"]["Year"]}')

            m_data = {"__tablename__": f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}',
                      "__table_args__": {'extend_existing': True},
                      "id": Column(Integer, primary_key=True),
                      "matchId": Column(String(50), ForeignKey('match.id'))}

            SQLconfig['MatchDataConfig']['Attributes'] = {k: eval(v) for k, v in
                                                          SQLconfig['MatchDataConfig']['Attributes'].items()}
            self.MatchDataObject = type(f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}', (Base,),
                                        {**SQLconfig['MatchDataConfig']['Attributes'], **m_data})
            Matches.data_list = relationship(f'MatchData{SQLconfig["MatchDataConfig"]["Year"]}',uselist=False)

        except FileNotFoundError:
            pass

    def getTBAData(self, event: str):
        headers = {'X-TBA-Auth-Key': self.config['TBA-Key'], 'If-Modified-Since': self.tbaLastModified}
        r = requests.get(f'https://www.thebluealliance.com/api/v3/event/{event}/matches', headers=headers)
        print(r)
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
            for d in ['alliances.blue.dq_team_keys','alliances.blue.team_keys','alliances.blue.surrogate_team_keys','alliances.red.dq_team_keys','alliances.red.team_keys','alliances.red.surrogate_team_keys']:
                try:
                    x = x.drop(labels=[d])
                except KeyError:
                    pass
            # x = self.MatchDataObject(**x.to_dict())
            # g = self.MatchDataObject(**x.to_dict())
            # self.session.add(g)
            # self.session.commit()
            self.addMatch(x['key'],row[1]['alliances.red.team_keys'],row[1]['alliances.blue.team_keys'],self.MatchDataObject(**x.to_dict()))


        # TODO: Finish Method with getMatch function

        return r

    def parseConfig(self):
        with open('config/config.json') as f:
            config = json.load(f)

        self.config = config

        headers = {'X-TBA-Auth-Key': self.config['TBA-Key'], 'If-Modified-Since': self.tbaLastModified}
        r = requests.get(f'https://www.thebluealliance.com/api/v3/event/{self.config["Year"]}vahay/matches',
                         headers=headers)
        data = pd.json_normalize(r.json())
        drop_list = ['videos','score_breakdown','alliances.blue.dq_team_keys','alliances.blue.team_keys','alliances.blue.surrogate_team_keys','alliances.red.dq_team_keys','alliances.red.team_keys','alliances.red.surrogate_team_keys']
        for d in drop_list:
            try:
                data = data.drop(d, axis=1)
            except KeyError:
                pass
        data = data.infer_objects()
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
                warnings.warn(f'{dtype} is not a configured datatype. It will not be used.')

        originalConfig = json.load(open('./config/SQLconfig.json', 'r'))
        with open('./config/SQLconfig.json', 'w') as f:
            originalConfig['MatchDataConfig']['Attributes'] = matchDataConfig
            json.dump(originalConfig, f, indent=4)


d = DataInput()
x = time_ns()
d.getTBAData('2020vahay')
print((time_ns()-x)*1e-9)
