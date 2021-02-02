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

