from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    BigInteger,
    ForeignKey,
    Integer,
    String,
    Boolean,
    Float,
    Text,
)
from sqlalchemy.orm import relationship, sessionmaker

# Setting Up SQL
Base = declarative_base()


class Matches(Base):
    __tablename__ = "match"
    id = Column(String(50), primary_key=True)
    data_list = relationship("MatchData")


class Teams(Base):
    __tablename__ = "team"
    id = Column(String(10), primary_key=True)
    data_list = relationship("TeamData")


class TeamData(Base):
    __tablename__ = "team_data"
    id = Column(Integer, primary_key=True)
    teamid = Column(String(50), ForeignKey("team.id"))


class MatchData(Base):
    __tablename__ = "match_data"
    id = Column(Integer, primary_key=True)
    matchId = Column(String(50), ForeignKey("match.id"))
