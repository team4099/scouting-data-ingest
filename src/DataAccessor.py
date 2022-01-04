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
    Warning,
    Info,
    Scout,
    Prediction,
    MatchDatum,
    TeamDatum,
    CalculatedTeamDatum,
    match_data_map,
    team_data_map,
)
from terminal import logger


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

    def get_all_alliances(
        self,
    ) -> Optional[List[Team]]:
        """
        Get all teams in a given alliance from a certain match
        
        """
        
        query = "SELECT teamid, matchid, color FROM alliances;"
        return list(self.session.execute(query).fetchall())
    
    def get_all_data_for_a_metric(
        self,
        metric: str
    ) -> Optional[List[MatchDatum]]:
        """
        
        Get all data for a specific metric for both red and blue alliances

        """
        query = f"SELECT match_id, r_{metric}, b_{metric} FROM match_data;"
        metricdata = list(self.session.execute(query).fetchall())
        hashoutput = {}
        for matchinfo in metricdata:
            hashoutput[matchinfo[0]] = (matchinfo[1], matchinfo[2])
        return hashoutput
        
    def get_all_match_objects(
        self,
        metrics: List[str]
    ) -> Optional[List[MatchDatum]]:
        query = self.session.query(MatchDatum).options(load_only(*metrics))
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
        all_alliance_info = []
        for alliance in red_alliances:
            all_alliance_info.append(alliance)
        for alliance in blue_alliances:
            all_alliance_info.append(alliance)
        return all_alliance_info
    
    def get_all_matches(
        self,
    ) -> Optional[List[Match]]:
        """
        
        Get all match ids

        """
        query = "SELECT match_id from match_data;"
        return [match[0] for match in list(self.session.execute(query).fetchall()) if len(match) > 0]

    def get_match(
        self,
        key: str
    ) -> Optional[Match]:
        """
        Get a match by id
        """
        query = self.session.query(Match).filter(Match.id == key).first()
        return query

    def get_team(self, id: str) -> Optional[Team]:
        """
        Get a team by id
        """
        query = self.session.query(Team).filter(Team.id == id).first()
        return query

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
        id: str,
    ) -> Optional[Info]:
        """
        Get a info by id
        """
        query = self.session.query(Info).filter(Info.id == id).first()
        return query

    def get_scouts(
        self,
        scout_id: Optional[str] = None,
        active: Optional[bool] = None,
    ) -> Optional[List[Scout]]:
        """
        Get a warning by id
        """
        query = self.session.query(Scout)
        if id:
            query = query.filter(Scout.id == scout_id)
        if active:
            query = query.filter(Scout.active == active)

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

    def get_match_datum(
        self,
        match_id: Optional[str] = None,
    ) -> Optional[MatchDatum]:
        """
        Get a match by id
        """
        query = (
            self.session.query(MatchDatum)
            .filter(MatchDatum.match_id == match_id)
            .first()
        )

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
        query = (
            self.session.query(CalculatedTeamDatum)
            .filter(CalculatedTeamDatum.team_id == team_id)
            .first()
        )

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
        active: Optional[Boolean] = None,
        points: int = 0,
        streak: int = 0,
    ) -> None:
        if not self.get_scouts(id, active):
            s = Scout(id=id, active=active, points=points, streak=streak)
            self.session.add(s)

    def add_prediction(
        self, scout_id: str, match_id: str, prediction: Alliance
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
        new_vars["winning_alliance"] = Alliance(match_json["winning_alliance"])
        new_vars["event_key"] = match_json["event_key"]
        new_vars["time"] = datetime.fromtimestamp(match_json["time"], pytz.utc)
        new_vars["actual_time"] = datetime.fromtimestamp(
            match_json["actual_time"], pytz.utc
        )
        new_vars["predicted_time"] = datetime.fromtimestamp(
            match_json["predicted_time"], pytz.utc
        )
        new_vars["post_result_time"] = datetime.fromtimestamp(
            match_json["post_result_time"], pytz.utc
        )

        # Dynamically set Year specific items
        for letter, color in zip(["r", "b"], ["red", "blue"]):
            for key, value in match_data_map.items():
                new_vars[f"{letter}_{key}"] = match_json[
                    f"score_breakdown.{color}.{value}"
                ]

            md.__dict__.update(new_vars)

        self.session.add(md)
        self.session.flush()

    def add_team_datum(
        self,
        team_id: str,
        # scout_id: str,
        match_id: str,
        alliance: Alliance,
        driver_station: int,
        team_datum_json: dict,
    ) -> None:
        # if (
        #    self.get_team(team_id) is None
        #    or self.get_team_data(match_id=match_id, team_id=team_id, alliance=alliance)
        #    != []
        # ):
        #    return None

        new_vars = {}
        td = TeamDatum(
            match_id=match_id,
            team_id=team_id,
            alliance=alliance,
            driver_station=driver_station,
        )

        new_vars["time"] = datetime.strptime(
            team_datum_json["Timestamp"], "%m/%d/%Y %H:%M:%S"
        ).replace(tzinfo=pytz.timezone("America/New_York"))

        #if type(team_datum_json["Climb Type"]) != Null:
        #    new_vars["final_climb_type"] = ClimbType(
        #        team_datum_json["Climb Type"]
        #    )

        # Dynamically set Year specific items
        for key, value in team_data_map.items():
            new_vars[key] = team_datum_json[value]

            td.__dict__.update(new_vars)

        self.session.add(td)
        self.session.flush()

    def add_calculated_team_datum(self, team_id: str, calculated_team_datum_json: dict):
        if self.get_team(team_id) is None:
            return None
        if (old_team_data := self.get_calculated_team_data(team_id)) is not None:
            new_vars = old_team_data.__dict__
            # Dynamically set Year specific items
            for key, value in calculated_team_datum_json.items():
                if type(value) == Null or value != new_vars[key]:
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

    def update_scout(
        self, scout_id: str, active: bool = None, points: int = None, streak: int = None
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
        query = self.session.query(Warnings)

        query = query.filter(Warnings.id == id)[0]

        query.ignore = ignore

        self.session.flush()
        self.session.commit()

    def delete_warnings(self):
        query = self.session.query(Warnings)
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
