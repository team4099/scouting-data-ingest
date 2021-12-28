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


class ClimbType(enum.Enum):
    hang = "hang"
    park = "park"
    no_climb = "no climb"
    none = "none"


# Declaring SQL Objects


class Match(Base):
    __tablename__ = "matches"
    id = Column(String(50), primary_key=True)

    match_data = relationship("MatchDatum", back_populates="match", uselist=False)
    warnings = relationship("Warning", back_populates="match")
    predictions = relationship("Prediction", back_populates="match")
    team_data = relationship("TeamDatum", back_populates="match")

    comp_level = Column(Enum(CompLevel))
    set_number = Column(Integer)
    match_number = Column(Integer)
    event_key = Column(String(50))

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
    calculated_team_data = relationship("CalculatedTeamDatum", back_populates="team")

    def __repr__(self) -> str:
        return f"<Team id={self.id}>"


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
    team_data = relationship("TeamDatum", back_populates="scout")

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

    r_init_line_1 = Column(String(50))
    r_init_line_2 = Column(String(50))
    r_init_line_3 = Column(String(50))
    r_endgame_1 = Column(String(50))
    r_endgame_2 = Column(String(50))
    r_endgame_3 = Column(String(50))
    r_auto_cells_bottom = Column(Integer)
    r_auto_cells_outer = Column(Integer)
    r_auto_cells_inner = Column(Integer)
    r_teleop_cells_bottom = Column(Integer)
    r_teleop_cells_outer = Column(Integer)
    r_teleop_cells_inner = Column(Integer)
    r_stage_1_activated = Column(Boolean)
    r_stage_2_activated = Column(Boolean)
    r_stage_3_activated = Column(Boolean)
    r_stage_3_color = String(20)
    r_endgame_level_rung = Column(String(50))
    r_auto_init_line_points = Column(Integer)
    r_auto_cell_points = Column(Integer)
    r_auto_points = Column(Integer)
    r_teleop_cell_points = Column(Integer)
    r_control_panel_points = Column(Integer)
    r_endgame_points = Column(Integer)
    r_teleop_points = Column(Integer)
    r_shield_operational_rp = Column(Boolean)
    r_shield_energized_rp = Column(Boolean)
    r_shield_energized_rp_from_foul = Column(Boolean)
    r_num_hanging = Column(Integer)
    r_foul_count = Column(Integer)
    r_tech_foul_count = Column(Integer)
    r_adjust_points = Column(Integer)
    r_foul_points = Column(Integer)
    r_rp = Column(Integer)
    r_total_points = Column(Integer)

    b_init_line_1 = Column(String(50))
    b_init_line_2 = Column(String(50))
    b_init_line_3 = Column(String(50))
    b_endgame_1 = Column(String(50))
    b_endgame_2 = Column(String(50))
    b_endgame_3 = Column(String(50))
    b_auto_cells_bottom = Column(Integer)
    b_auto_cells_outer = Column(Integer)
    b_auto_cells_inner = Column(Integer)
    b_teleop_cells_bottom = Column(Integer)
    b_teleop_cells_outer = Column(Integer)
    b_teleop_cells_inner = Column(Integer)
    b_stage_1_activated = Column(Boolean)
    b_stage_2_activated = Column(Boolean)
    b_stage_3_activated = Column(Boolean)
    b_stage_3_color = String(20)
    b_endgame_level_rung = Column(String(50))
    b_auto_init_line_points = Column(Integer)
    b_auto_cell_points = Column(Integer)
    b_auto_points = Column(Integer)
    b_teleop_cell_points = Column(Integer)
    b_control_panel_points = Column(Integer)
    b_endgame_points = Column(Integer)
    b_teleop_points = Column(Integer)
    b_shield_operational_rp = Column(Boolean)
    b_shield_energized_rp = Column(Boolean)
    b_shield_energized_rp_from_foul = Column(Boolean)
    b_num_hanging = Column(Integer)
    b_foul_count = Column(Integer)
    b_tech_foul_count = Column(Integer)
    b_adjust_points = Column(Integer)
    b_foul_points = Column(Integer)
    b_rp = Column(Integer)
    b_total_points = Column(Integer)

    def __repr__(self) -> str:
        return f"<MatchDatum id={self.id} match_id={self.match_id}>"


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

    auto_low_goal = Column(Integer)
    auto_high_goal = Column(Integer)
    auto_misses = Column(Integer)
    auto_notes = Column(Text)
    teleop_low_goal = Column(Integer)
    teleop_high_goal = Column(Integer)
    teleop_misses = Column(Integer)
    teleop_notes = Column(Text)

    control_panel = Column(Integer)

    from_initiation_line = Column(Boolean)
    from_target_zone = Column(Boolean)
    from_near_trench = Column(Boolean)
    from_rendezvous_point = Column(Boolean)
    from_far_trench = Column(Boolean)

    climb_time = Column(Integer)
    attempted_park = Column(Boolean)
    attempted_hang = Column(Boolean)
    final_climb_type = Column(Enum(ClimbType))

    notes = Column(Text)

    def __repr__(self) -> str:
        return f"<TeamDatum id={self.id} team_id={self.team_id} match_id={self.match_id} alliance={self.alliance} driver_station={self.driver_station}>"


class CalculatedTeamDatum(Base):
    __tablename__ = "calculated_team_data"
    id = Column(Integer, primary_key=True)
    team_id = Column(String(50), ForeignKey("teams.id"))
    team = relationship("Team", back_populates="calculated_team_data")

    # Year Specific Config

    auto_low_goal_avg = Column(Float)
    auto_high_goal_avg = Column(Float)
    auto_misses_avg = Column(Float)
    teleop_low_goal_avg = Column(Float)
    teleop_high_goal_avg = Column(Float)
    teleop_misses_avg = Column(Float)
    fouls_avg = Column(Float)
    climb_time_avg = Column(Float)

    auto_low_goal_med = Column(Float)
    auto_high_goal_med = Column(Float)
    auto_misses_med = Column(Float)
    teleop_low_goal_med = Column(Float)
    teleop_high_goal_med = Column(Float)
    teleop_misses_med = Column(Float)
    fouls_med = Column(Float)
    climb_time_med = Column(Float)

    from_target_zone_usage = Column(Float)
    from_initiation_line_usage = Column(Float)
    from_near_trench_usage = Column(Float)
    from_far_trench_usage = Column(Float)
    from_rendezvous_point_usage = Column(Float)

    hang_pct = Column(Float)
    park_pct = Column(Float)
    no_climb_pct = Column(Float)

    teleop_high_pct = Column(Float)
    teleop_low_pct = Column(Float)
    teleop_miss_pct = Column(Float)

    comments = Column(Text)


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


match_data_map = {
    "init_line_1": "initLineRobot1",
    "init_line_2": "initLineRobot2",
    "init_line_3": "initLineRobot3",
    "endgame_1": "endgameRobot1",
    "endgame_2": "endgameRobot2",
    "endgame_3": "endgameRobot3",
    "auto_cells_bottom": "autoCellsBottom",
    "auto_cells_outer": "autoCellsOuter",
    "auto_cells_inner": "autoCellsInner",
    "teleop_cells_bottom": "teleopCellsBottom",
    "teleop_cells_outer": "teleopCellsOuter",
    "teleop_cells_inner": "teleopCellsInner",
    "stage_1_activated": "stage1Activated",
    "stage_2_activated": "stage2Activated",
    "stage_3_activated": "stage3Activated",
    "endgame_level_rung": "endgameRungIsLevel",
    "auto_init_line_points": "autoInitLinePoints",
    "auto_cell_points": "autoInitLinePoints",
    "auto_points": "autoInitLinePoints",
    "teleop_cell_points": "autoInitLinePoints",
    "control_panel_points": "controlPanelPoints",
    "engdame_points": "autoInitLinePoints",
    "teleop_points": "teleopPoints",
    "shield_operation_rp": "shieldOperationalRankingPoint",
    "shield_energized_rp": "shieldEnergizedRankingPoint",
    "shield_energized_rp_from_foul": "tba_shieldEnergizedRankingPointFromFoul",
    "num_hanging": "tba_numRobotsHanging",
    "foul_count": "foulCount",
    "tech_foul_count": "techFoulCount",
    "adjust_points": "adjustPoints",
    "foul_points": "foulPoints",
    "rp": "rp",
    "total_points": "totalPoints",
}

team_data_map = {
    "auto_low_goal": "Auto Low Goal",
    "auto_high_goal": "Auto High Goal",
    # "auto_misses": "Auto Misses",
    # "auto_notes": "Auto Notes",
    "teleop_low_goal": "Teleop Low Goal",
    "teleop_high_goal": "Teleop High Goal",
    "teleop_misses": "Teleop Misses",
    # "teleop_notes": "Teleop Notes",
    "control_panel": "Control Panel",
    "from_initiation_line": "Initiation Line?",
    "from_target_zone": "Target Zone?",
    "from_near_trench": "Near Trench?",
    "from_far_trench": "Far Trench",
    "from_rendezvous_point": "Rendezvous point?",
    "climb_time": "Climb Time",
    #"attempted_park": "Attempted Park",
    "attempted_hang": "Climb Attempted",
    # "final_climb_type": "Final Climb Type",
    "notes": "Notes",
}
