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


class Teams(Base):
    __tablename__ = "team"
    id = Column(String(10), primary_key=True)

class Alliance(Base):
    __tablename__ = "alliances"
    id = Column(Integer(), primary_key=True)
    teamid = Column(String(10))
    matchid = Column(String(50))
    color = Column(String(10))

class Warnings(Base):
    __tablename__ = "warnings"
    id = Column(Integer(), primary_key=True)
    match = Column(String(50))
    alliance = Column(String(10))
    category = Column(String(50))
    content = Column(Text())
    ignore = Column(Boolean())

class Info(Base):
    __tablename__ = "info"
    id = Column(String(20), primary_key=True)
    value = Column(Text())

class Scouts(Base):
    __tablename__ = "scouts"
    id = Column(String(20), primary_key=True)
    active = Column(Boolean())
    points = Column(Integer())
    streak = Column(Integer())

class Predictions(Base):
    __tablename__ = "predictions"
    id = Column(Integer(), primary_key=True)
    scout = Column(String(20))
    match = Column(String(50))
    prediction = Column(String(10))
