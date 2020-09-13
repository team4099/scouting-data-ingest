from sqlalchemy import create_engine, Table, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, ForeignKey, Integer, String, Boolean
from sqlalchemy.orm import relationship, sessionmaker
import json

Base = declarative_base()

association_red_table = Table('red_association', Base.metadata,
                              Column('team_id', Integer, ForeignKey('team.id')),
                              Column('match_id', Integer, ForeignKey('match.id'))
                              )
association_blue_table = Table('blue_association', Base.metadata,
                               Column('team_id', Integer, ForeignKey('team.id')),
                               Column('match_id', Integer, ForeignKey('match.id'))
                               )


class Matches(Base):
    __tablename__ = 'match'
    id = Column(Integer, primary_key=True)
    red_teams = relationship("Teams",
                             secondary=association_red_table,
                             backref="r_matches")
    blue_teams = relationship("Teams",
                              secondary=association_blue_table,
                              backref="b_matches")
    data_list = relationship('MatchData')


class Teams(Base):
    __tablename__ = 'team'
    id = Column(Integer, primary_key=True)
    data_list = relationship('TeamData')


class TeamData(Base):
    __tablename__ = 'team_data'
    id = Column(Integer, primary_key=True)
    teamid = Column(Integer, ForeignKey('team.id'))
    match = Column(String(50))
    event = Column(String(50))


class MatchData(Base):
    __tablename__ = 'match_data'
    id = Column(Integer, primary_key=True)
    matchId = Column(Integer, ForeignKey('match.id'))
    winner = Column(Boolean)


class DataInput:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:robotics4099@localhost/scouting')  ## In Memory.
        self.Sessiontemplate = sessionmaker()
        self.Sessiontemplate.configure(bind=self.engine)
        self.session = self.Sessiontemplate()

        self.TeamDataObject = TeamData
        self.MatchDataObject = MatchData
        self.parseConfig()

        Base.metadata.create_all(self.engine)
        t1 = self.TeamDataObject(teamid=40999,match="q46",event="vahay",PowerCells=40)
        self.session.add(t1)
        self.session.commit()

    def addMatch(self, id: int, red_teams_num: list, blue_teams_num: list):
        if self.checkIfExists(Matches, id):
            raise Exception('Match already exists')
        if len(red_teams_num) > 3 or len(blue_teams_num) > 3:
            raise Exception('Team list cannot be bigger than 3')
        red_teams = [
            Teams(id=id) if not self.checkIfExists(Teams, id) else self.session.query(Teams).filter_by(id=id)[0]
            for id in red_teams_num]
        blue_teams = [
            Teams(id=id) if not self.checkIfExists(Teams, id) else self.session.query(Teams).filter_by(id=id)[0]
            for id in blue_teams_num]

        m = Matches(id=id, red_teams=red_teams, blue_teams=blue_teams)

        self.session.add(m)
        self.session.commit()

    def addTeam(self, id):
        if self.checkIfExists(Teams, id):
            raise Exception('Team already exists')

        t = Teams(id=id)
        self.session.add(t)
        self.session.commit()

    def getTeam(self,id):
        if self.checkIfExists(Teams,id):
            return self.session.query(Teams).filter_by(id=id)[0]
        else:
            raise Exception('Team does not exist')

    def checkIfExists(self, obj, id):
        (ret,), = self.session.query(exists().where(obj.id == id))
        return ret

    def parseConfig(self):
        try:

            with open('./SQLconfig.json') as f:
                config = json.load(f)

            t_data = {"__tablename__": f'TeamData{config["TeamDataConfig"]["Year"]}',
                      "id": Column(Integer, primary_key=True),
                      "teamid": Column(Integer, ForeignKey('team.id')),
                      "match": Column(String(50)),
                      "event": Column(String(50))}

            config['TeamDataConfig']['Attributes'] = {k: eval(v) for k, v in
                                                      config['TeamDataConfig']['Attributes'].items()}
            self.TeamDataObject = type(f'TeamData{config["TeamDataConfig"]["Year"]}', (Base,),
                                       {**config['TeamDataConfig']['Attributes'], **t_data})
            Teams.data_list = relationship(f'TeamData{config["TeamDataConfig"]["Year"]}')

            m_data = {"__tablename__": f'MatchData{config["MatchDataConfig"]["Year"]}',
                      "id": Column(Integer, primary_key=True),
                      "matchId": Column(Integer, ForeignKey('match.id')),
                      "winner": Column(Boolean)}

            config['MatchDataConfig']['Attributes'] = {k: eval(v) for k, v in
                                                       config['MatchDataConfig']['Attributes'].items()}
            self.MatchDataObject = type(f'MatchData{config["MatchDataConfig"]["Year"]}', (Base,),
                                        {**config['MatchDataConfig']['Attributes'], **m_data})
            Matches.data_list = relationship(f'MatchData{config["MatchDataConfig"]["Year"]}')

        except FileNotFoundError:
            pass


d = DataInput()
print(d.getTeam(40999).data_list)
