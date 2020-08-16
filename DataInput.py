from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, ForeignKey, Integer
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()

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
        self.Sessiontemplate.configure(bind=engine)
        self.session = self.Sessiontemplate()


association_red_table = Table('red_association', Base.metadata,
    Column('team_id', Integer, ForeignKey('team.id')),
    Column('match_id', Integer, ForeignKey('match.id'))
)
association_blue_table = Table('blue_association', Base.metadata,
    Column('team_id', Integer, ForeignKey('team.id')),
    Column('match_id', Integer, ForeignKey('match.id'))
)



t1 = Teams(id=1)
t2 = Teams(id=2)
t3 = Teams(id=3)
t4 = Teams(id=4)
m = Matches(id=1,red_teams=[t1,t2],blue_teams=[t3,t4])
r = Matches(id=2,red_teams=[t3,t4],blue_teams=[t1,t2])

Base.metadata.create_all(engine)

session.add(t1)
session.add(t2)
session.add(t3)
session.add(t4)
session.add(m)
session.add(r)
session.commit()

