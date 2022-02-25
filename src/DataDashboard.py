import datetime
import itertools
from flask import Flask, render_template, jsonify
from flask.globals import request
import re
from DataInput import DataInput
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
from SQLObjects import Alliance, Base

# send help I don't know how to organize a flask application

app = Flask(__name__)
app.config["SECRET_KEY"] = "Team4099!"
app.debug = True
config = Config(logger, False)


def update_data_accessor(data_accessor=None):
    engine = create_engine(
        f"mysql+pymysql://{config.db_user}:{config.db_pwd}@db/scouting"
    )
    session_template = sessionmaker()
    session_template.configure(bind=engine)
    session = session_template()
    connection = engine.connect()

    data_accessor.engine = engine
    data_accessor.session = scoped_session(session)
    data_accessor.connection = connection

    return data_accessor


engine = create_engine(f"mysql+pymysql://{config.db_user}:{config.db_pwd}@db/scouting")
session_template = sessionmaker()
session_template.configure(bind=engine)
s_session = scoped_session(session_template)
session = s_session
connection = engine.connect()
data_accessor = DataAccessor(engine, session, connection, config)
data_input = DataInput(engine, session, connection, data_accessor, config)
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


@app.route("/match/<key>")
def match(key):
    if not key.startswith(config.event):
        key = f"{config.year}{config.event}_{key}"
    return render_template(
        "match.html", match_key=key, key=key[key.index("_") + 1 :].upper()
    )


@app.route("/match/<key>/data")
def match_data(key):
    if not key.startswith(f"{config.year}{config.event}"):
        key = f"{config.year}{config.event}_{key}"

    data_acc = update_data_accessor(data_accessor)
    payload = {}
    match = data_acc.get_match(key=key)

    if (match_data := data_acc.get_match_datum(match_id=match)) is not None:
        payload["occurred"] = True
        payload["actualTime"] = match_data.actual_time
        payload["winner"] = match_data.winning_alliance
        score = {"red": {}, "blue": {}}
        for color in ["red", "blue"]:
            score[color]["totalScore"] = match.__dict__[f"{color[0]}_total_points"]
            score[color]["teleopScore"] = match[f"{color[0]}_teleop_points"]
            score[color]["autoScore"] = match[f"{color[0]}_auto_points"]
            score[color]["endgameScore"] = match[f"{color[0]}_endgame_points"]
            score[color]["rankingPoints"] = match[f"{color[0]}_rp"]
            score[color]["autoHighGoal"] = (
                match[f"{color[0]}_auto_cells_outer"]
                + match[f"{color[0]}_auto_cells_inner"]
            )
            score[color]["autoLowGoal"] = match[f"{color[0]}_auto_cells_lower"]
            score[color]["teleopHighGoal"] = (
                match[f"{color[0]}_teleop_cells_outer"]
                + match[f"{color[0]}_teleop_cells_inner"]
            )
            score[color]["teleopLowGoal"] = match[f"{color[0]}_teleop_cells_lower"]
            payload["score"] = score
            predictions = data_acc.get_predictions(match_id=key)
            payload["odds"] = {
                "red": sum([1 for p in predictions if p.prediction == Alliance.red])
                / len(predictions),
                "blue": sum([1 for p in predictions if p.prediction == Alliance.blue])
                / len(predictions),
            }
        else:
            payload["occurred"] = False
            red_alliance = [
                t.calculated_team_data for t in match.team if t.alliance == Alliance.red
            ]
            blue_alliance = [
                t.calculated_team_data
                for t in match.team
                if t.alliance == Alliance.blue
            ]
            data = {"red": {}, "blue": {}}
            for alliance, color in zip([red_alliance, blue_alliance], ["red", "blue"]):
                data[color]["autoHighGoal"] = sum(
                    [t.auto_high_goal_avg for t in alliance]
                )
                data[color]["autoLowGoal"] = sum(
                    [t.auto_low_goal_avg for t in alliance]
                )
                data[color]["teleopHighGoal"] = sum(
                    [t.teleop_high_goal_avg for t in alliance]
                )
                data[color]["teleopLowGoal"] = sum(
                    [t.teleop_low_goal_avg for t in alliance]
                )
                data[color]["teleopMisses"] = sum(
                    [t.teleop_misses_avg for t in alliance]
                )
                data[color]["endgameScore"] = sum(
                    [
                        t.Climb_Type_Park * 5 + t.Climb_Type_Hang * 25
                        for t in alliance
                        for t in teams
                        if t != [] and t.teamid in alliance[color]
                    ]
                )
                data[color]["climbTimeScore"] = sum(
                    [
                        30 - t.climb_time_avg
                        for t in alliance and t.climb_time_avg is not None
                    ]
                )


@app.route("/match/<key>/data2")
def match_data2(key):
    if not key.startswith(f"{config.year}{config.event}"):
        key = f"{config.year}{config.event}_{key}"

    payload = {}
    match = data_accessor.get_match_datum
    teams = [
        data_accessor.get_calculated_team_data(t.teamid, type_df=False)[0]
        for t in data_accessor.get_alliance(key, type_df=False)
    ]

    alliance = {}
    alliance["red"] = [
        t.teamid for t in data_accessor.get_alliance(key, "Red", type_df=False)
    ]
    alliance["blue"] = [
        t.teamid for t in data_accessor.get_alliance(key, "Blue", type_df=False)
    ]
    payload["alliance"] = alliance

    if match["actual_time"] != datetime.datetime.fromtimestamp(0):
        payload["occurred"] = True
        payload["actualTime"] = match["actual_time"]
        payload["winner"] = match["winning_alliance"]
        score = {"red": {}, "blue": {}}
        for color in ["red", "blue"]:
            score[color]["totalScore"] = match[f"alliances.{color}.score"]
            score[color]["teleopScore"] = match[f"score_breakdown.{color}.teleopPoints"]
            score[color]["autoScore"] = match[f"score_breakdown.{color}.autoPoints"]
            score[color]["endgameScore"] = match[
                f"score_breakdown.{color}.endgamePoints"
            ]
            score[color]["rankingPoints"] = match[f"score_breakdown.{color}.rp"]
            score[color]["autoHighGoal"] = (
                match[f"score_breakdown.{color}.autoCellsOuter"]
                + match[f"score_breakdown.{color}.autoCellsInner"]
            )
            score[color]["autoLowGoal"] = match[
                f"score_breakdown.{color}.autoCellsBottom"
            ]
            score[color]["teleopHighGoal"] = (
                match[f"score_breakdown.{color}.teleopCellsOuter"]
                + match[f"score_breakdown.{color}.teleopCellsInner"]
            )
            score[color]["teleopLowGoal"] = match[
                f"score_breakdown.{color}.teleopCellsBottom"
            ]
        payload["score"] = score
        payload["odds"] = (
            data_accessor.get_prediction(match=key)["prediction"]
            .value_counts(normalize=True)
            .to_dict()
        )
    else:
        payload["occurred"] = False
        payload["expectedTime"] = match["predicted_time"]
        data = {"red": {}, "blue": {}}
        for color in ["red", "blue"]:
            data[color]["autoHighGoal"] = sum(
                [
                    t.Auto_High_Goal_avg
                    for t in teams
                    if t != [] and t.teamid in alliance[color]
                ]
            )
            data[color]["autoLowGoal"] = sum(
                [
                    t.Auto_Low_Goal_avg
                    for t in teams
                    if t != [] and t.teamid in alliance[color]
                ]
            )
            data[color]["teleopHighGoal"] = sum(
                [
                    t.Teleop_High_Goal_avg
                    for t in teams
                    if t != [] and t.teamid in alliance[color]
                ]
            )
            data[color]["teleopLowGoal"] = sum(
                [
                    t.Teleop_Low_Goal_avg
                    for t in teams
                    if t != [] and t.teamid in alliance[color]
                ]
            )
            data[color]["teleopMisses"] = sum(
                [
                    t.Teleop_Misses_avg
                    for t in teams
                    if t != [] and t.teamid in alliance[color]
                ]
            )
            data[color]["endgameScore"] = sum(
                [
                    t.Climb_Type_Park * 5 + t.Climb_Type_Hang * 25
                    for t in teams
                    if t != [] and t.teamid in alliance[color]
                ]
            )
            data[color]["climbTimeScore"] = sum(
                [
                    30 - t.Climb_Time_avg
                    for t in teams
                    if t != []
                    and t.teamid in alliance[color]
                    and t.Climb_Time_avg is not None
                ]
            )
        payload["data"] = data

    return payload


@app.route("/team/<teamid>")
def team(teamid):
    if not teamid.startswith("frc"):
        teamid = "frc" + teamid
    return render_template("team.html", teamid=teamid)


@app.route("/team/<teamid>/data")
def team_data(teamid):
    if not teamid.startswith("frc"):
        teamid = "frc" + teamid

    payload = {}
    team = data_accessor.get_calculated_team_data(teamid, type_df=False)[0]
    payload["autoHighGoal"] = team.Auto_High_Goal_avg
    payload["autoLowGoal"] = team.Auto_Low_Goal_avg
    payload["teleopHighGoal"] = team.Teleop_High_Goal_avg
    payload["teleopLowGoal"] = team.Teleop_Low_Goal_avg
    payload["teleopMisses"] = team.Teleop_Misses_avg
    payload["endgameScore"] = team.Climb_Type_Park * 5 + team.Climb_Type_Hang * 25
    payload["parkClimb"] = team.Climb_Type_Park
    payload["hangClimb"] = team.Climb_Type_Hang
    payload["noClimb"] = team.Climb_Type_No_Climb
    payload["climbTimeScore"] = team.Climb_Time_avg

    matches = data_accessor.get_match_data(occured=False)
    team_matches = data_accessor.get_alliance(teamid=teamid)["matchid"].tolist()
    next_match = (
        matches[
            (matches["matchId"].isin(team_matches))
            & (matches["actual_time"] == datetime.datetime.fromtimestamp(0))
        ]
        .sort_values(by="predicted_time")["matchId"]
        .iloc[:2]
        .tolist()
    )
    print(next_match)
    payload["nextMatch"] = next_match
    payload["nextAlliance"] = [
        data_accessor.get_alliance(teamid=teamid, match_key=nm, type_df=False)[0].color
        for nm in next_match
    ]
    return payload


@app.route("/match/<key>/prediction", methods=["POST"])
def match_prediction(key):
    if not key.startswith(f"{config.year}{config.event}"):
        key = f"{config.year}{config.event}_{key}"

    match = data_accessor.get_match_data(
        match_key=key, type_df=True, occured=False
    ).loc[0]

    if match["actual_time"] != datetime.datetime.fromtimestamp(0):
        return "Invalid: Match Occurred"
    else:
        data = request.json
        data_accessor.add_prediction(data["scout"], key, data["prediction"])
        return "Valid"


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/scouts/scouts")
def scout_names():
    return {"names": data_accessor.get_scouts()["id"].to_list()}


@app.route("/scouts/change", methods=["POST"])
def change_scouts():
    data = request.json
    if len(data["add"]) > 0:
        data_accessor.add_scout(data["add"])
    if len(data["remove"]) > 0:
        data_accessor.delete_scout(data["remove"])
    if len(data["active"]) > 0:
        data_accessor.update_scout(data["active"], active=True)
    if len(data["inactive"]) > 0:
        data_accessor.update_scout(data["inactive"], active=False)
    return ""


@app.route("/scouts/data", methods=["GET", "POST"])
def scout_data():
    if col := request.args.get("sortBy"):
        return (
            data_accessor.get_scouts()
            .sort_values(by=col, axis=0, ascending=False)
            .to_html(index=False)
        )
    else:
        return data_accessor.get_scouts().to_html(index=False)


@app.route("/scouts", methods=["GET", "POST"])
def scouts():
    return render_template("scouts.html")


@app.route("/form", methods=["GET"])
def form():
    return render_template("form.html")

@app.route("/api/get_match_data", methods=["GET"])
def get_match_data():
    all_matches = [match_object.serialize for match_object in data_accessor.get_match_datum()]
    jsonoutput = {}
    for match in all_matches:
        match_id = list(match.keys())[0] # i cannot think of a more efficient way to do this
        jsonoutput[match_id] = match[match_id]
        all_teams_for_match = data_accessor.get_alliance_associations(
            match_id = match_id,
            dictionary = True
        )[match_id]
        jsonoutput[match_id]["currMatch"]["alliances"] = all_teams_for_match
        jsonoutput[match_id]["currMatchData"]["predictions"] = [] #TODO figure out predictions
        jsonoutput[match_id]["team_metrics"] = {}
        for team_id in all_teams_for_match["red"]:
            jsonoutput[match_id]["team_metrics"][team_id] = data_accessor.get_calculated_team_data(team_id = team_id).serialize[team_id[3:]]
            jsonoutput[match_id]["team_metrics"][team_id]["alliance"] = "red"
        for team_id in all_teams_for_match["blue"]:
            jsonoutput[match_id]["team_metrics"][team_id] = data_accessor.get_calculated_team_data(team_id = team_id).serialize[team_id[3:]]
            jsonoutput[match_id]["team_metrics"][team_id]["alliance"] = "blue"

        # for team_id in list(itertools.chain(*list(all_teams_for_match.values()))):
        #     jsonoutput[match_id]["team_metrics"][team_id] = data_accessor.get_calculated_team_data(team_id = team_id).serialize[team_id[3:]]

    return jsonoutput

@app.route("/api/match_keys", methods=["GET"])
def get_match_keys():
    return jsonify(
        [match.serialize["match_id"] for match in data_accessor.get_match()]
    )

@app.route("/api/team_ids", methods=["GET"])
def get_team_ids():
    return jsonify(
        [team.serialize["team_id"] for team in data_accessor.get_warnings()]
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
        scout_id = list(all_scouts.keys())[0]
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
    return data_accessor.get_calculated_team_data(team_id = teamid).serialize


# TODO write documentation on the correct POST request format :///
@app.route("/api/add_team_datum", methods=["POST"])
def add_team_datum():
    data = request.args
    # Year specific config

    climb_type_map = {
        "0": "hang",
        "1": "park",
        "2": "no_climb",
        "3": "none"
    }
    data_accessor.add_team_datum(
        team_id = "frc" + data.get("team_number"),
        # scout_id = data["scout_id"],
        match_id = data.get("match_key"),
        alliance = Alliance.red if data.get("alliance") == "red" else Alliance.blue,
        driver_station = data.get("driver_station"),
        team_datum_json = {
            "auto_low_goal": data.get("auto_low_goal"),
            "auto_high_goal": data.get("auto_high_goal"),
            "auto_misses": data.get("auto_misses"),
            "auto_notes": data.get("auto_notes"),
            "teleop_low_goal": data.get("teleop_low_goal"),
            "teleop_high_goal": data.get("teleop_high_goal"),
            "teleop_misses": data.get("teleop_misses"),
            "control_panel": data.get("control_panel"),
            "from_initiation_line": True if '0' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_target_zone": True if '1' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_near_trench": True if '2' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_rendezvous_point": True if '3' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "from_far_trench": True if '4' in data.get("shooting_zones") else False, # TODO need to find a better way do do this
            "teleop_notes": data.get("teleop_notes"),
            "climb_time": data.get("climb_time"),
            "attempted_park": bool(data.get("attempted_park")),
            "attempted_hang": bool(data.get("attempted_hang")),
            "final_climb_type": climb_type_map[str(data.get("final_climb_type"))]
        })
    data_accessor.engine.dispose()
    return "Worked"

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
    data = request.args
    data_accessor.session.rollback()
    data_accessor.update_warning(
        id = data["warning_id"],
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5001")
