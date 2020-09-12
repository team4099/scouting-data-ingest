from sqlalchemy import create_engine, Table, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, ForeignKey, Integer
from sqlalchemy.orm import relationship, sessionmaker

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


class Teams(Base):
    __tablename__ = 'team'
    id = Column(Integer, primary_key=True)


class DataInput:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:robotics4099@localhost/sqlalchemy')  ## In Memory.
        self.Sessiontemplate = sessionmaker()
        self.Sessiontemplate.configure(bind=self.engine)
        self.session = self.Sessiontemplate()

        Base.metadata.create_all(self.engine)

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

    def addTeam(self,id):
        if self.checkIfExists(Teams,id):
            raise Exception('Team already exists')

        t = Teams(id=id)
        self.session.add(t)
        self.session.commit()

    def checkIfExists(self, obj, id):
        (ret,), = self.session.query(exists().where(obj.id == id))
        return ret


d = DataInput()
d.addTeam(40999)
