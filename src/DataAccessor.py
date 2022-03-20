from datetime import datetime
import pytz
import pandas as pd
from sqlalchemy import exists, update
import copy
from typing import Union, Optional, List, Literal

from sqlalchemy import Boolean
from sqlalchemy.sql.elements import Null
from sqlalchemy.orm import load_only

from SQLObjects import (
    Alliance,
    ClimbType,
    CompLevel,
    Match,
    Team,
    AllianceAssociation,
    Warning,
    Info,
    Scout,
    PitScouting,
    Prediction,
    MatchDatum,
    TeamDatum,
    CalculatedTeamDatum,
    match_data_map,
    team_data_map,
)
from terminal import logger
from json import dumps


class DataAccessor:
    def __init__(self, engine, session, connection, config):
        self.log = logger.opt(colors=True)

        self.log.info("Starting DataAccessor")
        # Loading configuration
        self.log.info("Loading Configuration")

        self.config = config

        # Connecting to MySQL
        self.log.info("Connecting to MySQL")
        self.engine = engine
        self.session = session
        self.connection = connection

        self.log.info("Initializing Variables")
        self.warning_dict = {}
        self.last_checked = None

        self.log.info("DataAccessor Loaded!")
        
    def get_all_match_objects(
        self,
        metrics: Optional[List[str]] = None
    ) -> Optional[List[MatchDatum]]:
        """
        Gets specific metrics for all MatchDatum objects
        """
        query = self.session.query(MatchDatum)
        if metrics is not None:
            query = query.options(load_only(*metrics))
        return list(self.session.execute(query).fetchall())

    def get_all_alliance(
        self,
    ) -> Optional[List[TeamDatum]]:

        query = self.session.query(TeamDatum.match_id, TeamDatum.team_id, TeamDatum.alliance)
        all_alliances = list(self.session.execute(query).fetchall())
        allredteams = [teamdataobject[1] for teamdataobject in all_alliances if teamdataobject[2] == Alliance.red]
        allblueteams = [teamdataobject[1] for teamdataobject in all_alliances if teamdataobject[2] == Alliance.blue]
        red_alliances = list(zip(*[iter(allredteams)]*3))
        blue_alliances = list(zip(*[iter(allblueteams)]*3))
        return red_alliances + blue_alliances

    def get_match(
        self,
        key: Optional[str] = None
    ) -> Optional[Match]:
        """
        Get a match by id
        """
        query = self.session.query(Match)

        if key is not None:
            query = query.filter(Match.id == key).first()
        else:
            query = query.all()

        return query

    def get_team(
        self, 
        id: Optional[str] = None
    ) -> Optional[Team]:
        """
        Get a team by id
        """
        query = self.session.query(Team)

        if id is not None:
            query = query.filter(Team.id == id).first()
        else:
            query = query.all()

        return query

    def get_alliance_associations(
        self,
        match_id: Optional[str] = None,
        alliance: Optional[Alliance] = None,
        team_id: Optional[str] = None,
        driver_station: Optional[int] = None,
        json: Optional[Boolean] = False,
        dictionary: Optional[Boolean] = False
    ) -> Optional[List[AllianceAssociation]]:
        """
        Get a alliance association by id
        """
        query = self.session.query(AllianceAssociation)
        if match_id:
            query = query.filter(AllianceAssociation.match_id == match_id)
        if team_id:
            query = query.filter(AllianceAssociation.team_id == team_id)
        if alliance:
            query = query.filter(AllianceAssociation.alliance == alliance)
        if driver_station:
            query = query.filter(AllianceAssociation.driver_station == driver_station)
        
        alliance_associations = query.all() if query is not None else None
        if json or dictionary:
            alliancejsondict = {}
            for alliance in alliance_associations:
                if alliance.match_id not in alliancejsondict.keys():
                    alliancejsondict[alliance.match_id] = {"red":["","",""], "blue":["","",""]}
                alliancejsondict[alliance.match_id][alliance.alliance.value][alliance.driver_station-1] = alliance.team_id
            if json:
                return dumps(alliancejsondict)
            else:
                return alliancejsondict
        else:
            return alliance_associations 

    def get_warnings(
        self,
        match_id: Optional[str] = None,
        alliance: Optional[Alliance] = None,
        category: Optional[str] = None,
        ignore: Optional[bool] = None,
    ) -> Optional[List[Warning]]:
        """
        Get a warning by id
        """
        query = self.session.query(Warning)
        if match_id:
            query = query.filter(Warning.match_id == match_id)
        if alliance:
            query = query.filter(Warning.alliance == alliance)
        if category:
            query = query.filter(Warning.category == category)
        if ignore:
            query = query.filter(Warning.ignore == ignore)

        return query.all() if query is not None else None

    def get_info(
        self,
        field: str,
    ) -> Optional[Info]:
        """
        Get a info by field
        """
        query = self.session.query(Info).filter(Info.id == field).first()
        return query

    def get_scouts(
        self,
        scout_id: Optional[str] = None,
    ) -> Optional[List[Scout]]:
        """
        Get a warning by id
        """
        query = self.session.query(Scout)
        if scout_id is not None:
            query = query.filter(Scout.id == scout_id)
        return query.all() if query is not None else None

    def get_predictions(
        self,
        scout_id: Optional[str] = None,
        match_id: Optional[str] = None,
    ) -> Optional[List[Prediction]]:
        """
        Get a warning by id
        """
        query = self.session.query(Prediction)
        if scout_id:
            query = query.filter(Prediction.scout_id == scout_id)
        if match_id:
            query = query.filter(Prediction.match_id == match_id)

        return query.all() if query is not None else None

    def get_pit_scouting_datum(
        self,
        team_id: Optional[str] = None,
    ) -> Optional[List[PitScouting]]:
        """
        Get data from Pit Scouting for a given team or for all teams
        """
        query = self.session.query(PitScouting)
        if team_id:
            query = query.filter(PitScouting.team_id == team_id)
        
        return query.all() if query is not None else None


    def get_match_datum(
        self,
        match_id: Optional[str] = None,
    ) -> Optional[MatchDatum]:
        """
        Get a match by id
        """
        query = self.session.query(MatchDatum)

        if match_id is not None:
            query = query.filter(MatchDatum.match_id == match_id).first()
        else:
            query = query.all()

        return query

    def get_team_data(
        self,
        match_id: Optional[str] = None,
        team_id: Optional[str] = None,
        alliance: Optional[Alliance] = None,
        scout_id: Optional[str] = None,
    ) -> Optional[List[TeamDatum]]:
        """
        Get a alliance association by id
        """
        query = self.session.query(TeamDatum)
        if match_id:
            query = query.filter(TeamDatum.match_id == match_id)
        if team_id:
            query = query.filter(TeamDatum.team_id == team_id)
        if alliance:
            query = query.filter(TeamDatum.alliance == alliance)
        if scout_id:
            query = query.filter(TeamDatum.scout_id == scout_id)

        return query.all() if query is not None else None

    def get_calculated_team_data(
        self,
        team_id: Optional[str] = None,
    ) -> Optional[CalculatedTeamDatum]:
        """
        Get a alliance association by id
        """
        self.session.flush()
        query = (
            self.session.query(CalculatedTeamDatum)
            .filter(CalculatedTeamDatum.team_id == team_id)
            .first()
        )

        return query
    
    def temp_calc_team_data(
        self,
        team_id: Optional[str] = None,
    ) -> Optional[CalculatedTeamDatum]:
        query = self.session.query(CalculatedTeamDatum)

        if team_id is not None:
            query = query.filter(CalculatedTeamDatum.team_id == team_id).first()
        else:
            query = query.all()

        return query

    def add_match(
        self,
        id: str,
        comp_level: CompLevel,
        set_number: int,
        match_number: int,
        event_key: str,
    ) -> None:
        if not self.get_match(key=id):
            m = Match(
                id=id,
                comp_level=CompLevel(comp_level),
                set_number=set_number,
                match_number=match_number,
                event_key=event_key,
            )
            self.session.add(m)

    def add_team(self, id: str) -> None:
        if not self.get_team(id):
            t = Team(id=id)
            self.session.add(t)

    def add_pit_scouting(
        self,
        team_id: str,
        programming_language: str, #TODO figure out if we want to store as List of enums in code or separator (if it's purely used for front end stuff it honestly doesn't need to be converted imo)
        num_of_batteries: int,
        robot_info: str,
        rungs: str,
        other_info: str
    ) -> None:
        if not self.get_pit_scouting_datum(team_id):
            p = PitScouting(
                    team_id = team_id,
                    programming_language = programming_language,
                    num_of_batteries = num_of_batteries,
                    robot_info = robot_info,
                    rungs = rungs,
                    other_info = other_info
                )
            self.session.add(p)

    def add_warning(
        self,
        match_id: str,
        alliance: Alliance,
        category: str,
        content: str,
        ignore: Union[Boolean, Literal[False]] = False,
    ) -> None:
        if not self.get_warnings(match_id, alliance, category):
            w = Warning(
                match_id=match_id,
                alliance=alliance,
                category=category,
                content=content,
                ignore=ignore,
            )
            self.session.add(w)

    def add_info(self, id: str, value: str) -> None:
        if not self.get_info(id):
            i = Info(id=id, value=value)
            self.session.add(i)

    def add_scout(
        self,
        id: str,
        points: int = 0,
        streak: int = 0,
    ) -> None:
        if not self.get_scouts(id):
            s = Scout(id=id, points=points, streak=streak)
            self.session.add(s)

    def add_prediction(
        self,
        scout_id: str,
        match_id: str,
        prediction: Alliance
    ) -> None:
        if not self.get_predictions(scout_id=scout_id, match_id=match_id):
            p = Prediction(scout_id=scout_id, match_id=match_id, prediction=prediction)
            self.session.add(p)

    def add_match_datum(
        self,
        match_id: str,
        match_json: dict,
    ) -> None:
        check_match = self.get_match(key=match_id)
        if not check_match:
            return None

        new_vars = {}
        md = MatchDatum(match_id=match_id)

        new_vars["comp_level"] = CompLevel(match_json["comp_level"])
        new_vars["set_number"] = match_json["set_number"]
        new_vars["match_number"] = match_json["match_number"]
        new_vars["set_number"] = match_json["set_number"]
        new_vars["winning_alliance"] = Alliance(match_json["winning_alliance"]) if match_json["winning_alliance"] in ["red","blue"] else None
        new_vars["event_key"] = match_json["event_key"]
        new_vars["time"] = datetime.fromtimestamp(match_json["time"], pytz.utc)
        new_vars["actual_time"] = datetime.fromtimestamp(
            match_json["actual_time"], pytz.utc
        )
        #new_vars["predicted_time"] = datetime.fromtimestamp(
        #    match_json["predicted_time"], pytz.utc
        #)
        new_vars["post_result_time"] = datetime.fromtimestamp(
            match_json["post_result_time"], pytz.utc
        )

        def climb_tf(climb):
            if climb == ClimbType.none:
                return ClimbType.none
            return climb

        new_vars["r_endgame_1"] = climb_tf(ClimbType(match_json["score_breakdown.red.endgameRobot1"].lower()))
        new_vars["r_endgame_2"] = climb_tf(ClimbType(match_json["score_breakdown.red.endgameRobot2"].lower()))
        new_vars["r_endgame_3"] = climb_tf(ClimbType(match_json["score_breakdown.red.endgameRobot3"].lower()))
        new_vars["b_endgame_1"] = climb_tf(ClimbType(match_json["score_breakdown.blue.endgameRobot1"].lower()))
        new_vars["b_endgame_2"] = climb_tf(ClimbType(match_json["score_breakdown.blue.endgameRobot2"].lower()))
        new_vars["b_endgame_3"] = climb_tf(ClimbType(match_json["score_breakdown.blue.endgameRobot3"].lower()))

        # Dynamically set Year specific items
        for letter, color in zip(["r", "b"], ["red", "blue"]):
            for key, value in match_data_map.items():
                new_vars[f"{letter}_{key}"] = match_json[
                    f"score_breakdown.{color}.{value}"
                ]

            md.__dict__.update(new_vars)

        self.session.add(md)
        self.session.flush()

    def add_alliance_association(
        self,
        match_id: str,
        alliance: Alliance,
        team_id: str,
        driver_station: int
    ) -> None:
        aa = AllianceAssociation(
            match_id=match_id,
            team_id=team_id,
            alliance=alliance,
            driver_station=driver_station
        )

        self.session.add(aa)
        self.session.flush()


    def add_team_datum(
        self,
        team_id: str,
        scout_id: str,
        match_id: str,
        alliance: Alliance,
        driver_station: int,
        team_datum_json: dict,
    ) -> None:
        if (
           self.get_team(team_id) is None or 
           self.get_team_data(match_id=match_id, team_id=team_id, alliance=alliance)
           != []
        ):
            return None

        if self.get_scouts(scout_id) == []:
            self.add_scout(scout_id)
            self.session.flush()

        new_vars = {}
        td = TeamDatum(
            match_id=match_id,
            scout_id=scout_id,
            team_id=team_id,
            alliance=alliance,
            driver_station=driver_station,
        )

        
        new_vars["final_climb_type"] = ClimbType(team_datum_json["final_climb_type"].lower())

        td.__dict__.update(team_datum_json)

        self.session.add(td)
        self.session.commit()


    def add_calculated_team_datum(self, team_id: str, calculated_team_datum_json: dict):
        if self.get_team(team_id) is None:
            return None
        if (old_team_data := self.get_calculated_team_data(team_id)) is not None:
            new_vars = old_team_data.__dict__
            # Dynamically set Year specific items
            for key, value in calculated_team_datum_json.items():
                if type(value) == Null or value != new_vars.get(key):
                    new_vars[key] = value

            old_team_data.__dict__.update(new_vars)

            self.session.commit()
        else:
            new_vars = {}
            ctd = CalculatedTeamDatum(
                team_id=team_id,
            )

            # Dynamically set Year specific items
            for key, value in calculated_team_datum_json.items():
                new_vars[key] = value

            ctd.__dict__.update(new_vars)

            self.session.add(ctd)
            self.session.flush()

    def get_all_teams_df(self):
        return pd.read_sql_query(
            self.session.query(Team).statement, self.sql_connection()
        )
    

    def get_all_team_data_df(self):
        return pd.read_sql_query(
            self.session.query(TeamDatum).statement, self.sql_connection()
        )

    def update_prediction(self, scout_id: str, match_id: str, prediction: Alliance):
        prediction = self.get_predictions(scout_id, match_id)[0]
        prediction.prediction = prediction
        self.session.commit()

    def update_pit_scouting_datum(
        self,
        team_id: str,
        programming_language: str = None,
        num_of_batteries: int = None,
        robot_info: str = None,
        rungs: str = None,
        other_info: str = None
    ) -> None:
        pit_scouting_datum = self.get_pit_scouting_datum(team_id=team_id)

        if programming_language is not None:
            pit_scouting_datum.programming_language = programming_language
        
        if num_of_batteries is not None:
            pit_scouting_datum.num_of_batteries = num_of_batteries

        if robot_info is not None:
            pit_scouting_datum.robot_info = robot_info

        if rungs is not None:
            pit_scouting_datum.rungs = rungs

        if other_info is not None:
            pit_scouting_datum.other_info = other_info
        
        self.session.commit()

    def update_scout(
        self,
        scout_id: str,
        active: bool = None,
        points: int = None,
        streak: int = None
    ):
        scout = self.get_scouts(scout_id=scout_id)

        if active is not None:
            scout.active = active

        if points is not None:
            scout.points = points

        if streak is not None:
            scout.points = streak

        self.session.commit()

    def delete_scout(self, name):
        query = self.session.query(Scouts).filter(Scouts.id == name)
        query.delete()
        self.session.commit()

    def delete_match(self, match_key):
        query = self.session.query(self.MatchDataObject).filter(
            self.MatchDataObject.matchId == match_key
        )
        query.delete()
        self.session.flush()
        self.session.commit

    def process_predictions(self, match, result):
        query = self.session.query(Predictions)

        query = query.filter(Predictions.match == match)[:]
        for prediction in query:
            scout = self.session.query(Scouts).filter(Scouts.id == prediction.scout)[0]
            if prediction.prediction == result:
                scout.points += 10 + scout.streak * 0.5
                scout.streak += 1
            else:
                scout.streak = 0
        self.session.commit()

    def update_warning(self, id, ignore):
        query = self.session.query(Warning)

        query = query.filter(Warning.id == id)[0]

        query.ignore = ignore

        self.session.flush()
        self.session.commit()

    def delete_warnings(self):
        query = self.session.query(Warning)
        query.delete()

    def update_info(self, id, value):
        query = self.session.query(Info)

        query = query.filter(Info.id == id)[0]

        query.value = value

        self.session.commit()

    def delete_calculated_team_data(self, team_id=None):
        """
        Deletes a CalculatedTeamData Object from the database.

        :param team_id: A specific Team ID to be deleted
        :type team_id: str
        :rtype: None
        """
        query = self.session.query(self.CalculatedTeamDataObject)
        if team_id is not None:
            query = query.filter(self.CalculatedTeamDataObject.teamid == team_id)

        query.delete()

    def delete_team_data(self, team_id=None, match_key=None):
        """
        Deletes a TeamData Object from the database.

        :param team_id: A specific Team ID to be deleted
        :type team_id: str
                :param team_id: A specific Team ID to be deleted
        :type team_id: str
        :rtype: None
        """
        query = self.session.query(self.TeamDataObject)
        if team_id is not None:
            query = query.filter(self.TeamDataObject.teamid == team_id)
        if match_key is not None:
            query = query.filter(self.TeamDataObject.Match_Key == match_key)

        query.delete()

    def sql_connection(self):
        return self.engine.connect()
