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
    Enum,
)
from sqlalchemy.orm import relationship, sessionmaker
import enum

# Setting Up SQL
Base = declarative_base()


class Alliance(enum.Enum):
    red = "red"
    blue = "blue"


class Match(Base):
    __tablename__ = "matches"
    id = Column(String(50), primary_key=True)
    match_data = relationship("MatchDatum", back_populates="match", uselist=False)

    warnings = relationship("Warning", back_populates="match")
    predictions = relationship("Prediction", back_populates="match")

    alliance_associations = relationship("AllianceAssociation", back_populates="match")

    def __repr__(self) -> str:
        return f"<Match id={self.id}>"

    def get_red_alliance(self):
        return sorted(
            [a for a in self.alliance_associations if a.alliance == Alliance.red],
            key=lambda x: x.driver_station,
        )

    def get_blue_alliance(self):
        return sorted(
            [a for a in self.alliance_associations if a.alliance == Alliance.blue],
            key=lambda x: x.driver_station,
        )


class Team(Base):
    __tablename__ = "teams"
    id = Column(String(10), primary_key=True)
    team_data = relationship("TeamDatum", back_populates="team")
    alliance_associations = relationship("AllianceAssociation", back_populates="team")

    def __repr__(self) -> str:
        return f"<Team id={self.id}>"


class AllianceAssociation(Base):
    __tablename__ = "alliance_associations"
    id = Column(Integer, primary_key=True)

    match_id = Column(String(50), ForeignKey("matches.id", name="match_id"))
    match = relationship(
        "Match",
        foreign_keys=[match_id],
        back_populates="alliance_associations",
    )

    team_id = Column(String(10), ForeignKey("teams.id"))
    team = relationship("Team", back_populates="alliance_associations")

    alliance = Column(Enum(Alliance))
    driver_station = Column(Integer)

    def __repr__(self) -> str:
        return f"<AllianceAssociation id={self.id} match_id={self.match_id} team_id={self.team_id} alliance={self.alliance} driver_station={self.driver_station}>"


class Warning(Base):
    __tablename__ = "warnings"
    id = Column(Integer, primary_key=True)
    match_id = Column(String(50), ForeignKey("matches.id"))
    match = relationship("Match", back_populates="warnings")
    alliance = Column(Enum(Alliance))
    category = Column(String(50))
    content = Column(Text)
    ignore = Column(Boolean)

    def __repr__(self) -> str:
        return f"<Warning match={self.match} alliance={self.alliance} category={self.category} content={self.content} ignore={self.ignore}>"


class Info(Base):
    __tablename__ = "infos"
    id = Column(String(20), primary_key=True)
    value = Column(Text)

    def __repr__(self) -> str:
        return f"<Info id={self.id} value={self.value}>"


class Scout(Base):
    __tablename__ = "scouts"
    id = Column(String(20), primary_key=True)
    active = Column(Boolean, default=True)
    points = Column(Integer, default=0)
    streak = Column(Integer, default=0)
    predictions = relationship("Prediction", back_populates="scout")

    def __repr__(self) -> str:
        return f"<Scout id={self.id} active={self.active} points={self.points} streak={self.streak}>"


class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer(), primary_key=True)
    scout_id = Column(String(20), ForeignKey("scouts.id"))
    scout = relationship("Scout", back_populates="predictions")
    match_id = Column(String(50), ForeignKey("matches.id"))
    match = relationship("Match", back_populates="predictions")
    prediction = Column(Enum(Alliance))

    def __repr__(self) -> str:
        return f"<Prediction id={self.id} scout={self.scout} match={self.match} prediction={self.prediction}>"


# Define MatchDatum and TeamDatum Object below
# Each object should follow according to TBA's schema and the form's questions respectively


class MatchDatum(Base):
    __tablename__ = "match_data"
    id = Column(Integer, primary_key=True)
    match_id = Column(String(50), ForeignKey("matches.id"))
    match = relationship("Match", back_populates="match_data")

    def __repr__(self) -> str:
        return f"<MatchDatum id={self.id} match_id={self.match_id}>"


class TeamDatum(Base):
    __tablename__ = "team_data"
    id = Column(Integer, primary_key=True)
    team_id = Column(String(50), ForeignKey("teams.id"))
    team = relationship("Team", back_populates="team_data")

    def __repr__(self) -> str:
        return f"<TeamDatum id={self.id} team_id={self.team_id}>"
