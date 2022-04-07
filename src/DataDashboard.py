import datetime
import itertools
from flask import Flask, render_template, jsonify
from flask.globals import request
import re
from DataCalculator import DataCalculator
import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    ForeignKey,
    Float,
    null,
)
from sqlalchemy.orm import sessionmaker,scoped_session
from Config import Config
from DataAccessor import DataAccessor
from loguru import logger
import json
from SQLObjects import Alliance, Base, Defense
from flask_cors import CORS
from waitress import serve

# send help I don't know how to organize a flask application

app = Flask(__name__)
CORS(app, CORS_ORIGINS="*")
app.config["SECRET_KEY"] = "Team4099!"
app.config['CORS_HEADERS'] = 'Content-Type'
config = Config(logger, False)


def update_data_accessor(data_accessor=None):
    engine = create_engine(
        f"mysql+pymysql://{config.db_user}:{config.db_pwd}@{config.db_host}/scouting"
    )
    session_template = sessionmaker()
    session_template.configure(bind=engine)
    session = session_template()
    connection = engine.connect()

    data_accessor.engine = engine
    data_accessor.session = session
    data_accessor.connection = connection

    return data_accessor


engine = create_engine(f"mysql+pymysql://{config.db_user}:{config.db_pwd}@{config.db_host}/scouting")
session_template = sessionmaker()
session_template.configure(bind=engine)
session = scoped_session(session_template)
connection = engine.connect()
data_accessor = DataAccessor(engine, session, connection, config)
calculated_team_data_object = None
alliance_info = data_accessor.get_alliance_associations(json=True)


@app.route("/warnings", methods=["GET", "POST"])
def warnings():
    warnings = update_data_accessor(data_accessor).get_warnings()
    if request.method == "GET":
        categories = set([m.category for m in warnings])
        grouped_warnings = {
            c: [m.content for m in warnings if m.category == c] for c in categories
        }
        data_accessor.engine.dispose()
        return grouped_warnings
    else:
        d = request.json
        for i in d["ignore"]:
            data_accessor.update_warning(i, 1)
        for i in d["watch"]:
            data_accessor.update_warning(i, 0)
        data_accessor.engine.dispose()
        return ""


@app.route("/status")
def get_status():
    data_a = update_data_accessor(data_accessor)
    info = {
        "Last Match": data_a.get_info("Last Match").value,
        "Status": data_a.get_info("Status").value,
        "Task": data_a.get_info("Task").value,
    }
    return info

@app.route("/api/get_match_data", methods=["GET"])
def get_match_data():
    all_associations = data_accessor.get_alliance_associations(
            dictionary = True
        )
    jsonoutput = {}
    all_match_data = [match_object.serialize for match_object in data_accessor.get_match_datum()]
    for match in all_match_data:
        match_id = list(match.keys())[0] # i cannot think of a more efficient way to do this
        all_teams_for_match = all_associations[match_id]
        jsonoutput[match_id] = {"currMatch":{"alliances":all_teams_for_match}, "currMatchData":{}}
        predictions_list = data_accessor.get_predictions(match_id=match_id)
        jsonoutput[match_id]["currMatchData"]["predictions"] = [sum([1 for i in predictions_list if i.prediction == Alliance.red])/(len(predictions_list) if len(predictions_list) else 1),sum([1 for i in predictions_list if i.prediction == Alliance.blue])/(len(predictions_list) if len(predictions_list) else 1)] #TODO figure out predictions
        jsonoutput[match_id]["currMatchData"] = match[match_id]["currMatchData"]

        # for team_id in list(itertools.chain(*list(all_teams_for_match.values()))):
        #     jsonoutput[match_id]["team_metrics"][team_id] = data_accessor.get_calculated_team_data(team_id = team_id).serialize[team_id[3:]]

    return jsonoutput

@app.route("/api/teams_in_match", methods=["GET"])
def teams_in_match():
    return {(match.comp_level.value + str(match.match_number)):data_accessor.get_alliance_associations(match.id, dictionary=True)[match.id] for match in data_accessor.get_match()}


@app.route("/api/match_keys", methods=["GET"])
def get_match_keys():
    return jsonify(
        [match.serialize["match_id"] for match in data_accessor.get_match()]
    )

@app.route("/api/team_ids", methods=["GET"])
def get_team_ids():
    return jsonify(
        [team.id for team in data_accessor.get_team()]
    )

@app.route("/api/match_ids", methods=["GET"])
def get_match_ids():
    return jsonify(
        [match.id for match in data_accessor.get_match()]
    )

@app.route("/api/occurred_match_ids", methods=["GET"])
def get_occurred_match_ids():
    return jsonify(
        [match_data.match.id for match_data in data_accessor.get_match_datum()]
    )

@app.route("/api/pit_scouting_data", methods=["GET"])
def get_pit_scouting():
    return jsonify(
        [pit_scouting_datum.serialize for pit_scouting_datum in data_accessor.get_pit_scouting_datum()]
    )


@app.route("/api/get_all_warnings", methods=["GET"])
def get_all_warnings():
    all_warnings = [warning.serialize for warning in data_accessor.get_warnings()]
    jsonoutput = {}
    for warning in all_warnings:
        warning_id = list(warning.keys())[0]
        jsonoutput[warning_id] = warning[warning_id]
    return jsonoutput

@app.route("/api/get_all_scouts", methods=["GET"]) #can't test but im confident this works
def get_all_scouts():
    all_scouts = [scout.serialize for scout in data_accessor.get_scouts()]
    jsonoutput = {}
    for scout in all_scouts:
        scout_id = list(scout.keys())[0]
        jsonoutput[scout_id] = scout[scout_id]
    return jsonoutput

@app.route("/api/status", methods=["GET"])
def get_api_status():
    return {
        "Last Match": data_accessor.get_info("Last Match").serialize["Last Match"],
        "Status": data_accessor.get_info("Status").serialize["Status"]
    }

@app.route("/api/teamdatum/<teamid>", methods=["GET"])
def get_team_datum(teamid):
    if len(teamid) < 3 or "frc" != teamid[0:3]:
        teamid = "frc" + teamid
    occurred_matches = [match_data.match.id for match_data in data_accessor.get_match_datum()]
    team_matches = [{"key":alliance.match_id} for alliance in data_accessor.get_alliance_associations(team_id=teamid) if alliance.match_id not in occurred_matches]
    data = data_accessor.get_calculated_team_data(team_id = teamid).serialize
    data["next_matches"] = team_matches
    return data


# TODO write documentation on the correct POST request format :///
@app.route("/api/add_team_datum", methods=["POST"])
def add_team_datum():
    data_accessor.session.commit()
    data = request.json
    # Year specific config

    climb_type_map = {
        "0": "none",
        "1": "low",
        "2": "mid",
        "3": "high",
        "4": "traversal"
    }
    if not isinstance(data["auto_shooting_zones"],list):
        data["auto_shooting_zones"] = [data["auto_shooting_zones"]]
    if not isinstance(data["shooting_zones"],list):
        data["shooting_zones"] = [data["shooting_zones"]]
    data_accessor.add_team_datum(
        team_id =  str(data.get("team_number")),
        scout_id = data.get("scout_id"),
        match_id = data.get("match_key"),
        alliance = Alliance.red if data.get("alliance") == "red" else Alliance.blue,
        driver_station = data.get("driver_station"),
        team_datum_json = {
            "preloaded_cargo": bool(data.get("preloaded_cargo")),
            "auto_lower_hub": data.get("auto_lower_hub"),
            "auto_upper_hub": data.get("auto_upper_hub"),
            "auto_misses": data.get("auto_misses"),
            "auto_human_scores": data.get("auto_human_score"),
            "auto_human_misses": data.get("auto_human_misses"),
            "taxied": bool(data.get("taxied")),
            "auto_notes": data.get("auto_notes"),
            "teleop_lower_hub": data.get("teleop_lower_hub"),
            "teleop_upper_hub": data.get("teleop_upper_hub"),
            "teleop_misses": data.get("teleop_misses"),
            "from_fender": True if '0' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_opposing_fender": True if '1' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_tarmac": True if '2' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_opposing_tarmac": True if '3' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_launchpad": True if '4' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_terminal": True if '5' in data.get("shooting_zones") else False, 
            "from_elsewhere": True if '6' in data.get("shooting_zones") else False, 
            "auto_from_fender": True if '0' in data.get("auto_shooting_zones") else False, # TODO need to find a better way do do this
            "auto_from_opposing_fender": True if '1' in data.get("auto_shooting_zones") else False, # TODO need to find a better way do do this
            "auto_from_tarmac": True if '2' in data.get("auto_shooting_zones") else False, # TODO need to find a better way do do this
            "auto_from_opposing_tarmac": True if '3' in data.get("auto_shooting_zones") else False, # TODO need to find a better way do do this
            "auto_from_launchpad": True if '4' in data.get("auto_shooting_zones") else False, # TODO need to find a better way do do this
            "auto_from_terminal": True if '5' in data.get("auto_shooting_zones") else False,
            "auto_from_elsewhere": True if '6' in data.get("auto_shooting_zones") else False, 
            "teleop_notes": data.get("teleop_notes"),
            "attempted_low": data.get("attempted_low"),
            "low_rung_climb_time": data.get("low_climb_time"),
            "attempted_mid": data.get("attempted_mid"),
            "mid_rung_climb_time": data.get("mid_climb_time"),
            "attempted_high": data.get("attempted_high"),
            "high_rung_climb_time": data.get("high_climb_time"),
            "attempted_traversal": data.get("attempted_traversal"),
            "traversal_rung_climb_time": data.get("traversal_climb_time"),
            "defense_pct": data.get("defense_pct"),
            "counter_defense_pct": data.get("counter_defense_pct"),
            "defense_rating": data.get("defense_rating"),
            "counter_defense_rating": data.get("counter_defense_rating"),
            "driver_rating": data.get("driver_rating"),
            "final_climb_type": climb_type_map[str(data.get("final_climb_type"))]
        })
    data_accessor.session.commit()
    return ""

@app.route("/api/add_pit_scouting_datum", methods=["POST"])
def add_pit_scouting():
    data = request.args
    data_accessor.add_pit_scouting(
        team_id = data["team_id"],
        programming_language = data["programming_language"],
        num_of_batteries = data["num_of_batteries"],
        robot_info = data["robot_info"],
        rungs = data["rungs"],
        other_info = data["other_info"]
    )
    return ""

@app.route("/api/add_prediction", methods=["POST"])
def add_prediction():
    data = request.args
    data_accessor.add_prediction(
        scout_id = data["scout"],
        match_id = data["match"],
        prediction = Alliance.red if data["prediction"] == "red" else Alliance.blue,
    )
    return ""

@app.route("/api/change_warning", methods=["POST"])
def change_warning():
    update_data_accessor(data_accessor)
    data = request.args
    data_accessor.update_warning(
        id = int(data["warning_id"]),
        ignore = bool(data["ignore"])
    )
    data_accessor.engine.dispose()
    return ""

@app.route("/api/change_scout", methods=["POST"])
def change_scout():
    data = request.args
    data_accessor.update_scout(
        scout_id = data["scout_id"],
        active = bool(data["active"])
    )
    return ""

@app.route("/api/add_scout", methods=["POST"])
def add_scout(id):
    data = request.args
    data_accessor.add_scout(data["scout_id"])
    return ""

if __name__ == "__main__":
    serve(app,host="0.0.0.0", port="5001")
