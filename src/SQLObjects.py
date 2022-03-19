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
    DateTime,
    null,
)
from sqlalchemy.orm import relationship, sessionmaker
import enum


# Setting Up SQL
Base = declarative_base()


# Declaring Enums
class Alliance(enum.Enum):
    red = "red"
    blue = "blue"
    NA = "NA"


class CompLevel(enum.Enum):
    qm = "qm"
    ef = "ef"
    qf = "qf"
    sf = "sf"
    f = "f"

class Defense(enum.Enum):
    never = "never"
    sometimes = "sometimes"
    most_of_the_time = "most of the time"
    all_of_the_time = "all of the time"

class ProgrammingLanguages(enum.Enum):
    java = "Java"
    cpp = "C++"
    labview = "LabVIEW"
    kotlin = "Kotlin"
    python = "Python"


# class ClimbType(enum.Enum):
#     hang = "hang"
#     park = "park"
#     no_climb = "no climb"
#     none = "none"

class ClimbType(enum.Enum):
    traversal = "traversal"
    high = "high"
    mid = "mid"
    low = "low"
    none = "none"


# Declaring SQL Objects
class Team(Base):
    __tablename__ = "teams"
    id = Column(String(10), primary_key=True)

    team_data = relationship("TeamDatum", back_populates="team")
    calculated_team_data = relationship("CalculatedTeamDatum", back_populates="team")
    # pit_scouting = relationship("PitScouting", back_populates="team")
    alliance_associations = relationship("AllianceAssociation", back_populates="team")

    def __repr__(self) -> str:
        return f"<Team id={self.id}>"
    
    @property
    def serialize(self):
        return {
            "team_id": self.id
        }

class Match(Base):
    __tablename__ = "matches"
    id = Column(String(50), primary_key=True)

    match_data = relationship("MatchDatum", back_populates="match", uselist=False)
    warnings = relationship("Warning", back_populates="match")
    predictions = relationship("Prediction", back_populates="match")
    team_data = relationship("TeamDatum", back_populates="match")
    alliance_associations = relationship("AllianceAssociation", back_populates="match")

    comp_level = Column(Enum(CompLevel))
    set_number = Column(Integer)
    match_number = Column(Integer)
    event_key = Column(String(50))

    def __repr__(self) -> str:
        return f"<Match id={self.id}>"

    @property
    def serialize(self):
       """Return object data in easily serializable format"""
       return {
           "match_id": self.id,
            "comp_level": self.comp_level,
            "match_number": self.match_number,
            "event_key": self.event_key
       }


class AllianceAssociation(Base):
    __tablename__ = "alliance_associations"
    id = Column(Integer, primary_key=True)

    match_id = Column(String(50), ForeignKey("matches.id", name="match_id"))
    match = relationship(
        "Match",
        foreign_keys=[match_id],
        back_populates="alliance_associations",
    )

    team_id = Column(String(10), ForeignKey("teams.id", name="team_id"))
    team = relationship(
        "Team",
        foreign_keys = [team_id],
        back_populates="alliance_associations"
    )

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
    ignore = Column(Boolean, default=False)

    def __repr__(self) -> str:
        return f"<Warning match={self.match} alliance={self.alliance} category={self.category} content={self.content} ignore={self.ignore}>"

    @property
    def serialize(self):
        return {
            self.id: {
                "match_key": self.match_id,
                "alliance": "red" if self.alliance == Alliance.red else "blue",
                "category": self.category,
                "content": self.content,
                "ignore": self.ignore
            }
        }

class Info(Base):
    __tablename__ = "infos"
    id = Column(String(20), primary_key=True)
    value = Column(Text)

    def __repr__(self) -> str:
        return f"<Info id={self.id} value={self.value}>"
    
    @property
    def serialize(self):
        return {
            self.id: self.value
        }

class PitScouting(Base):
    __tablename__ = "pit_scouting"
    team_id = Column(String(10), primary_key = True) # TODO if this needs to be connected to teams figure out auto incrementing problem otherwise just keep it like this so it doesn't cause problems
    programming_language = Column(String(100)) #string with separator
    num_of_batteries = Column(Integer)
    robot_info = Column(String(100))
    rungs = Column(String(100)) #string with separator
    other_info = Column(String(100))

    def __repr__(self) -> str:
        return f"<Pit Scouting id = {self.id} team_id ={self.team_id} programming language = {self.programming_language} number of batteries = {self.num_of_batteries} robot information = {self.robot_info} rungs = {self.rungs} other information = {self.other_info}"
    
    @property
    def serialize(self):
        return {
            self.team_id: {
                "programming_languages": self.programming_language,
                "num_of_batteries": self.num_of_batteries,
                "robot_info": self.robot_info,
                "rungs": self.robot_info,
                "other_info": self.other_info
            }
        }


class Scout(Base):
    __tablename__ = "scouts"
    id = Column(String(20), primary_key=True)
    points = Column(Integer, default=0)
    streak = Column(Integer, default=0)
    predictions = relationship("Prediction", back_populates="scout")
    team_data = relationship("TeamDatum", back_populates="scout")

    def __repr__(self) -> str:
        return f"<Scout id={self.id} points={self.points} streak={self.streak}>"
    
    @property
    def serialize(self):
        return {
            self.id: {
                "points": self.points,
                "streak": self.streak
            }
        }


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

    # @property
    # def serialize(self):
    #     return {

    #     }    

# Define MatchDatum, TeamDatum, and CalculatedTeamDatum Object below
# Each object should follow according to TBA's schema and the form's questions respectively


class MatchDatum(Base):
    __tablename__ = "match_data"
    id = Column(Integer, primary_key=True)
    match_id = Column(String(50), ForeignKey("matches.id"))
    match = relationship("Match", back_populates="match_data")

    # Year agnostic config

    winning_alliance = Column(Enum(Alliance))
    time = Column(DateTime(timezone=True))
    actual_time = Column(DateTime(timezone=True))
    predicted_time = Column(DateTime(timezone=True))
    post_result_time = Column(DateTime(timezone=True))

    # Year specific config

    r_preloaded_cargo_robot_1 = Column(Integer)
    r_preloaded_cargo_robot_2 = Column(Integer)
    r_preloaded_cargo_robot_3 = Column(Integer)
    r_taxi_robot_1 = Column(String(50))
    r_taxi_robot_2 = Column(String(50))
    r_taxi_robot_3 = Column(String(50))
    r_endgame_1 = Column(Enum(ClimbType))
    r_endgame_2 = Column(Enum(ClimbType))
    r_endgame_3 = Column(Enum(ClimbType))
    r_auto_cargo_lower_near = Column(Integer)
    r_auto_cargo_lower_far = Column(Integer)
    r_auto_cargo_lower_blue = Column(Integer)
    r_auto_cargo_lower_red = Column(Integer)
    r_auto_cargo_upper_near = Column(Integer)
    r_auto_cargo_upper_far = Column(Integer)
    r_auto_cargo_upper_blue = Column(Integer)
    r_auto_cargo_upper_red = Column(Integer)
    r_auto_cargo_total = Column(Integer)
    r_teleop_cargo_lower_near = Column(Integer)
    r_teleop_cargo_lower_far = Column(Integer)
    r_teleop_cargo_lower_blue = Column(Integer)
    r_teleop_cargo_lower_red = Column(Integer)
    r_teleop_cargo_upper_near = Column(Integer)
    r_teleop_cargo_upper_far = Column(Integer)
    r_teleop_cargo_upper_blue = Column(Integer)
    r_teleop_cargo_upper_red = Column(Integer)
    r_teleop_cargo_total = Column(Integer)
    r_match_cargo_total = Column(Integer)
    r_auto_taxi_points = Column(Integer)
    r_auto_cargo_points = Column(Integer)
    r_auto_points = Column(Integer)
    r_quintet_achieved = Column(Boolean)
    r_teleop_cargo_points = Column(Integer)
    r_endgame_points = Column(Integer)
    r_teleop_points = Column(Integer)
    r_cargo_bonus_ranking_point = Column(Boolean)
    r_hangar_bonus_ranking_point = Column(Boolean)
    r_foul_count = Column(Integer)
    r_tech_foul_count = Column(Integer)
    r_adjust_points = Column(Integer)
    r_foul_points = Column(Integer)
    r_rp = Column(Integer)
    r_total_points = Column(Integer)

    b_preloaded_cargo_robot_1 = Column(Integer)
    b_preloaded_cargo_robot_2 = Column(Integer)
    b_preloaded_cargo_robot_3 = Column(Integer)
    b_taxi_robot_1 = Column(String(50))
    b_taxi_robot_2 = Column(String(50))
    b_taxi_robot_3 = Column(String(50))
    b_endgame_1 = Column(Enum(ClimbType))
    b_endgame_2 = Column(Enum(ClimbType))
    b_endgame_3 = Column(Enum(ClimbType))
    b_auto_cargo_lower_near = Column(Integer)
    b_auto_cargo_lower_far = Column(Integer)
    b_auto_cargo_lower_blue = Column(Integer)
    b_auto_cargo_lower_red = Column(Integer)
    b_auto_cargo_upper_near = Column(Integer)
    b_auto_cargo_upper_far = Column(Integer)
    b_auto_cargo_upper_blue = Column(Integer)
    b_auto_cargo_upper_red = Column(Integer)
    b_teleop_cargo_lower_near = Column(Integer)
    b_teleop_cargo_lower_far = Column(Integer)
    b_teleop_cargo_lower_blue = Column(Integer)
    b_teleop_cargo_lower_red = Column(Integer)
    b_teleop_cargo_upper_near = Column(Integer)
    b_teleop_cargo_upper_far = Column(Integer)
    b_teleop_cargo_upper_blue = Column(Integer)
    b_teleop_cargo_upper_red = Column(Integer)
    b_match_cargo_total = Column(Integer)
    b_auto_taxi_points = Column(Integer)
    b_auto_cargo_points = Column(Integer)
    b_auto_points = Column(Integer)
    b_quintet_achieved = Column(Boolean)
    b_teleop_cargo_points = Column(Integer)
    b_endgame_points = Column(Integer)
    b_teleop_points = Column(Integer)
    b_cargo_bonus_ranking_point = Column(Boolean)
    b_hangar_bonus_ranking_point = Column(Boolean)
    b_foul_count = Column(Integer)
    b_tech_foul_count = Column(Integer)
    b_adjust_points = Column(Integer)
    b_foul_points = Column(Integer)
    b_rp = Column(Integer)
    b_total_points = Column(Integer)

    def __repr__(self) -> str:
        return f"<MatchDatum id={self.id} match_id={self.match_id}>"

    @property
    def serialize(self):
       """Return object data in easily serializable format"""
       return {
            self.match_id: {
               "currMatchData": {
                   "postResultTime": self.post_result_time,
                   "predictions": [], # TODO figure out predictions
                   "winner": self.winning_alliance.value,
                   "metrics": {
                   "Auto Low Cargo": {
                    "red": self.r_auto_cargo_lower_near + self.r_auto_cargo_lower_far +self.r_auto_cargo_lower_blue +self.r_auto_cargo_lower_red,
                    "blue": self.r_auto_cargo_lower_near + self.r_auto_cargo_lower_far +self.r_auto_cargo_lower_blue +self.b_auto_cargo_lower_red
                },
                "Auto Upper Cargo": {
                    "red": self.r_auto_cargo_upper_near + self.r_auto_cargo_upper_far +self.r_auto_cargo_upper_blue +self.r_auto_cargo_upper_red,
                    "blue": self.b_auto_cargo_upper_near + self.b_auto_cargo_upper_far +self.b_auto_cargo_upper_blue +self.b_auto_cargo_upper_red
                },
                "Teleop Low Cargo": {
                    "red": self.r_teleop_cargo_lower_near + self.r_teleop_cargo_lower_far +self.r_teleop_cargo_lower_blue +self.r_teleop_cargo_lower_red,
                    "blue": self.b_teleop_cargo_lower_near + self.b_teleop_cargo_lower_far +self.b_teleop_cargo_lower_blue +self.b_teleop_cargo_lower_red
                },
                "Teleop Upper Cargo": {
                    "red": self.r_teleop_cargo_upper_near + self.r_teleop_cargo_upper_far +self.r_teleop_cargo_upper_blue +self.r_teleop_cargo_upper_red,
                    "blue": self.b_teleop_cargo_upper_near + self.b_teleop_cargo_upper_far +self.b_teleop_cargo_upper_blue +self.b_teleop_cargo_upper_red
                },
                "Endgame Points": {
                    "red": self.r_endgame_points,
                    "blue": self.b_endgame_points
                },
               "Foul Points": {
                   "red": self.r_foul_points,
                   "blue": self.b_foul_points
               },
               "Total Points": {
                   "red": self.r_total_points,
                   "blue": self.b_total_points
               }}
               },
                
           }
       }

class TeamDatum(Base):
    __tablename__ = "team_data"
    id = Column(Integer, primary_key=True)
    team_id = Column(String(50), ForeignKey("teams.id"))
    team = relationship("Team", back_populates="team_data")
    scout_id = Column(String(20), ForeignKey("scouts.id"))
    scout = relationship("Scout", back_populates="team_data")
    match_id = Column(String(50), ForeignKey("matches.id"))
    match = relationship("Match", back_populates="team_data")

    # Year agnostic config

    time = Column(DateTime(timezone=True))
    alliance = Column(Enum(Alliance))
    driver_station = Column(Integer)

    # Year Specific Config
    
    preloaded_cargo = Column(Boolean)
    auto_lower_hub = Column(Integer)
    auto_upper_hub = Column(Integer)
    auto_misses = Column(Integer)
    auto_human_scores = Column(Integer)
    auto_human_misses = Column(Integer)
    taxied = Column(Boolean)
    auto_from_fender = Column(Boolean)
    auto_from_elsewhere_in_tarmac = Column(Boolean)
    auto_from_launchpad = Column(Boolean)
    auto_from_terminal = Column(Boolean)
    auto_from_hangar_zone = Column(Boolean)
    auto_from_elsewhere_on_field = Column(Boolean)
    auto_from_opponent_tarmac = Column(Boolean)
    auto_notes = Column(Text)

    teleop_lower_hub = Column(Integer)
    teleop_upper_hub = Column(Integer)
    teleop_misses = Column(Integer)
    teleop_notes = Column(Text)
    
    from_fender = Column(Boolean)
    from_elsewhere_in_tarmac = Column(Boolean)
    from_launchpad = Column(Boolean)
    from_terminal = Column(Boolean)
    from_hangar_zone = Column(Boolean)
    from_elsewhere_on_field = Column(Boolean)
    from_opponent_tarmac = Column(Boolean)

    attempted_low = Column(Boolean)
    low_rung_climb_time = Column(Integer)
    attempted_mid = Column(Boolean)
    mid_rung_climb_time = Column(Integer)
    attempted_high = Column(Boolean)
    high_rung_climb_time = Column(Integer)
    attempted_traversal = Column(Boolean)
    traversal_rung_climb_time = Column(Integer)
    final_climb_type = Column(Enum(ClimbType))

    defense = Column(Integer)
    driver_rating = Column(Integer)

    notes = Column(Text)

    def __repr__(self) -> str:
        return f"<TeamDatum id={self.id} team_id={self.team_id} match_id={self.match_id} alliance={self.alliance} driver_station={self.driver_station}>"
    
    @property
    def serialize(self):
        return {
            "team_id": self.team_id
        }

    


class CalculatedTeamDatum(Base):
    __tablename__ = "calculated_team_data"
    id = Column(Integer, primary_key=True)
    team_id = Column(String(50), ForeignKey("teams.id"))
    team = relationship("Team", back_populates="calculated_team_data")

    # Year Specific Config

    auto_lower_hub_avg = Column(Float)
    auto_upper_hub_avg = Column(Float)
    auto_misses_avg = Column(Float)
    teleop_lower_hub_avg = Column(Float)
    teleop_upper_hub_avg = Column(Float)
    teleop_misses_avg = Column(Float)
    fouls_avg = Column(Float)
    low_rung_climb_time_avg = Column(Float)
    mid_rung_climb_time_avg = Column(Float)
    high_rung_climb_time_avg = Column(Float)
    traversal_rung_climb_time_avg = Column(Float)

    auto_lower_hub_med = Column(Float)
    auto_upper_hub_med = Column(Float)
    auto_misses_med = Column(Float)
    teleop_lower_hub_med = Column(Float)
    teleop_upper_hub_med = Column(Float)
    teleop_misses_med = Column(Float)
    fouls_med = Column(Float)
    low_rung_climb_time_med = Column(Float)
    mid_rung_climb_time_med = Column(Float)
    high_rung_climb_time_med = Column(Float)
    traversal_rung_climb_time_med = Column(Float)

    from_fender_usage = Column(Float)
    from_elsewhere_in_tarmac_usage = Column(Float)
    from_launchpad_usage = Column(Float)
    from_terminal_usage = Column(Float)
    from_hangar_zone_usage = Column(Float)
    from_elsewhere_on_field_usage = Column(Float)
    from_opponent_tarmac = Column(Float)

    auto_from_fender_usage = Column(Float)
    auto_from_elsewhere_in_tarmac_usage = Column(Float)
    auto_from_launchpad_usage = Column(Float)
    auto_from_terminal_usage = Column(Float)
    auto_from_hangar_zone_usage = Column(Float)
    auto_from_elsewhere_on_field_usage = Column(Float)
    auto_from_opponent_tarmac = Column(Float)

    attempted_low_usage = Column(Float)
    attempted_mid_usage = Column(Float)
    attempted_high_usage = Column(Float)
    attempted_traversal_usage = Column(Float)

    none_pct = Column(Float)
    low_rung_pct = Column(Float)
    mid_rung_pct = Column(Float)
    high_rung_pct = Column(Float)
    traversal_rung_pct = Column(Float)

    auto_upper_hub_pct = Column(Float)
    auto_lower_hub_pct = Column(Float)
    auto_miss_pct = Column(Float)
    teleop_upper_hub_pct = Column(Float)
    teleop_lower_hub_pct = Column(Float)
    teleop_miss_pct = Column(Float)
    

    comments = Column(Text)
    defense_time_avg = Column(Float)
    driver_rating_avg = Column(Float)

    @property
    def serialize(self):
       """Return object data in easily serializable format"""
       return {
           self.team_id[3:] : {
               "accuracy": {
                   "upper": self.teleop_upper_hub_pct,
                   "lower": self.teleop_lower_hub_pct,
                   "miss": self.teleop_miss_pct
               },
               "auto": {
                   "upper": self.auto_upper_hub_avg,
                   "lower": self.auto_lower_hub_avg,
                   "misses": self.auto_misses_avg,
               },
               "climb": {
                   "traversal": self.traversal_rung_pct,
                   "high": self.high_rung_pct,
                   "mid": self.mid_rung_pct,
                   "low": self.low_rung_pct,
                   "no_climb": self.none_pct
               },
               "climb_time" :{
                   "low_rung_climb_time": self.low_rung_climb_time_avg,
                   "mid_rung_climb_time": self.mid_rung_climb_time_avg,
                   "high_rung_climb_time": self.high_rung_climb_time_avg,
                   "traversal_rung_climb_time": self.traversal_rung_climb_time_avg,
                   
               },
               "attempted_climbs": {
                "attempted_low_usage": self.attempted_low_usage,
                "attempted_mid_usage":self.attempted_mid_usage,
                "attempted_high_usage":self.attempted_high_usage,
                "attempted_traversal_usage":self.attempted_traversal_usage
               },
               "misc": {
                   "fouls": self.fouls_avg
               },
               "teleop": {
                   "upper": self.teleop_upper_hub_avg,
                   "lower": self.teleop_lower_hub_avg,
                   "misses": self.teleop_misses_avg
               },
               "zones": {
                   "fender": self.from_fender_usage,
                   "elsewhere_in_tarmac": self.from_elsewhere_in_tarmac_usage,
                   "launchpad": self.from_launchpad_usage,
                   "terminal": self.from_terminal_usage,
                   "hangar_zone": self.from_hangar_zone_usage,
                   "elsewhere": self.from_elsewhere_on_field_usage,
                    "opponent": self.from_opponent_tarmac
               },
               "next_matches": [
                   
               ]
           }
       }


def flatten_json(json):
    flat = {}
    for key, value in json.items():
        if isinstance(value, dict):
            processed = flatten_json(value)
            for new_key, new_value in processed.items():
                flat[f"{key}.{new_key}"] = new_value
        else:
            if value is None:
                value = null()
            flat[key] = value
    return flat

# TODO do both of these
match_data_map = {
    "auto_cargo_lower_near": "autoCargoLowerNear",
    "auto_cargo_lower_far": "autoCargoLowerFar",
    "auto_cargo_lower_blue": "autoCargoLowerBlue",
    "auto_cargo_lower_red": "autoCargoLowerRed",
    "auto_cargo_upper_near": "autoCargoUpperNear",
    "auto_cargo_upper_far": "autoCargoUpperFar",
    "auto_cargo_upper_blue": "autoCargoUpperBlue",
    "auto_cargo_upper_red": "autoCargoUpperRed",
    "auto_cargo_total": "autoCargoTotal",
    "teleop_cargo_lower_near": "teleopCargoLowerNear",
    "teleop_cargo_lower_far": "teleopCargoLowerFar",
    "teleop_cargo_lower_blue": "teleopCargoLowerBlue",
    "teleop_cargo_lower_red": "teleopCargoLowerRed",
    "teleop_cargo_upper_near": "teleopCargoUpperNear",
    "teleop_cargo_upper_far": "teleopCargoUpperFar",
    "teleop_cargo_upper_blue": "teleopCargoUpperBlue",
    "teleop_cargo_upper_red": "teleopCargoUpperRed",
    "teleop_cargo_total": "teleopCargoTotal",
    "match_cargo_total": "matchCargoTotal",
    "auto_taxi_points": "autoTaxiPoints",
    "auto_cargo_points": "autoCargoPoints",
    "auto_points": "autoPoints",
    "quintet_achieved": "quintetAchieved",
    "teleop_cargo_points": "teleopCargoPoints",
    "endgame_points": "endgamePoints",
    "teleop_points": "teleopPoints",
    "cargo_bonus_ranking_point": "cargoBonusRankingPoint",
    "hangar_bonus_ranking_point": "hangarBonusRankingPoint",
    "foul_count": "foulCount",
    "tech_foul_count": "techFoulCount",
    "adjust_points": "adjustPoints",
    "foul_points": "foulPoints",
    "rp": "rp",
    "total_points": "totalPoints",
}

team_data_map = {
    "auto_lower_hub": "Auto Lower Hub",
    "auto_upper_hub": "Auto Upper Hub",
    "auto_misses": "Auto Misses",
    "auto_notes": "Auto Notes",
    "teleop_lower_hub": "Teleop Lower Hub",
    "teleop_upper_hub": "Teleop Upper Hub",
    "teleop_misses": "Teleop Misses",
    "teleop_notes": "Teleop Notes",
    "from_fender": "Fender?",
    "from_elsewhere_in_tarmac": "Elsewhere in Tarmac?",
    "from_launchpad": "Launchpad?",
    "from_terminal": "Terminal",
    "from_hangar_zone": "Hangar Zone?",
    "from_elsewhere_on_field": "Elsewhere on Field?",
    "low_rung_climb_time": "Low Rung Climb Time",
    "mid_rung_climb_time": "Mid Rung Climb Time",
    "high_rung_climb_time": "High Rung Climb Time",
    "traversal_rung_climb_time": "Traversal Rung Climb Time",
    "final_climb_type": "Final Climb Type",
    "notes": "Notes",
}
