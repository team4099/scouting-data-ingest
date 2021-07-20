import datetime
from flask import Flask, render_template
from flask.globals import request
import re
from DataInput import DataInput
from DataCalculator import DataCalculator
import pandas as pd
from sqlalchemy import create_engine,Column, Integer, String, Text, ForeignKey, Float, null
from sqlalchemy.orm import sessionmaker
from Config import Config
from DataAccessor import DataAccessor
from loguru import logger
import json
from SQLObjects import Base
# send help I don't know how to organize a flask application

app = Flask(__name__)
app.config["SECRET_KEY"] = "Team4099!"
app.debug = True
config = Config(logger,False)
engine = create_engine(f'mysql+pymysql://{config.db_user}:{config.db_pwd}@db/scouting')
session_template = sessionmaker()
session_template.configure(bind=engine)
session = session_template()
connection = engine.connect()
data_accessor = DataAccessor(engine,session,connection,config)
data_input = DataInput(engine,session,connection,data_accessor, config)
calculated_team_data_object = None

with open("CalculatedTeamData2020.json","r") as f:
        t_data = {
            "__tablename__": f'CalculatedTeamData{config.year}',
            "__table_args__": {"extend_existing": True},
        }
        calc_data_config = json.load(f)
        calc_data_config = {
            k: eval(v) for k, v in calc_data_config.items()
        }
        calculated_team_data_object = type(
            f'CalculatedTeamData{config.year}',
            (Base,),
            {**calc_data_config, **t_data},
        )

data_accessor.CalculatedTeamDataObject = calculated_team_data_object

@app.route("/warnings", methods=["GET", "POST"])
def warnings():
    warnings = data_accessor.get_warnings()
    if request.method == 'GET':
        categories = warnings["category"].unique()
        grouped_warnings = warnings.groupby("category")
        return {cat: grouped_warnings.get_group(cat).to_dict(orient="records") for cat in categories}
    else:
        d = request.json
        for i in d['ignore']:
            data_accessor.update_warning(i,1)
        for i in d['watch']:
            data_accessor.update_warning(i,0)
        return ""

@app.route("/status")
def get_status():
    info = data_accessor.get_info()
    return {entry["id"]:entry["value"] for entry in info.to_dict(orient="records")}

@app.route("/match/<key>")
def match(key):
    if not key.startswith(config.event):
        key = f"{config.year}{config.event}_{key}"
    return render_template("match.html", key=key)

@app.route("/match/<key>/data")
def match_data(key):
    if not key.startswith(f"{config.year}{config.event}"):
        key = f"{config.year}{config.event}_{key}"

    payload = {}
    match = data_accessor.get_match_data(match_key=key, type_df=True,occured=False).loc[0]
    teams = [data_accessor.get_calculated_team_data(t.teamid,type_df=False)[0] for t in data_accessor.get_alliance(key,type_df=False)]

    alliance = {}
    alliance["red"] = [t.teamid for t in data_accessor.get_alliance(key,"Red", type_df=False)]
    alliance["blue"] = [t.teamid for t in data_accessor.get_alliance(key,"Blue", type_df=False)]
    payload["alliance"] = alliance

    if match["actual_time"] != datetime.datetime.fromtimestamp(0):
        payload["occurred"] = True
        payload["actualTime"] = match["actual_time"]
        payload["winner"] = match["winning_alliance"]
        score = {"red":{}, "blue":{}}
        for color in ["red","blue"] :
            score[color]["totalScore"] = match[f"alliances.{color}.score"]
            score[color]["teleopScore"] = match[f"score_breakdown.{color}.teleopPoints"]
            score[color]["autoScore"] = match[f"score_breakdown.{color}.autoPoints"]
            score[color]["endgameScore"] = match[f"score_breakdown.{color}.endgamePoints"]
            score[color]["rankingPoints"] = match[f"score_breakdown.{color}.rp"]
            score[color]["autoHighGoal"] = match[f"score_breakdown.{color}.autoCellsOuter"] + match[f"score_breakdown.{color}.autoCellsInner"]
            score[color]["autoLowGoal"] = match[f"score_breakdown.{color}.autoCellsBottom"]
            score[color]["teleopHighGoal"] = match[f"score_breakdown.{color}.teleopCellsOuter"] + match[f"score_breakdown.{color}.teleopCellsInner"]
            score[color]["teleopLowGoal"] = match[f"score_breakdown.{color}.teleopCellsBottom"]
        payload["score"] = score
    else:
        payload["occurred"] = False
        payload["expectedTime"] = match["predicted_time"]
        data = {"red":{}, "blue":{}}
        for color in ["red","blue"]:
            data[color]["autoHighGoal"] = sum([t.Auto_High_Goal_avg for t in teams if t != [] and t.teamid in alliance[color]])
            data[color]["autoLowGoal"] = sum([t.Auto_Low_Goal_avg for t in teams if t != [] and t.teamid in alliance[color]])
            data[color]["teleopHighGoal"] = sum([t.Teleop_High_Goal_avg for t in teams if t != [] and t.teamid in alliance[color]])
            data[color]["teleopLowGoal"] = sum([t.Teleop_Low_Goal_avg for t in teams if t != [] and t.teamid in alliance[color]])
            data[color]["teleopMisses"] = sum([t.Teleop_Misses_avg for t in teams if t != [] and t.teamid in alliance[color]])
            data[color]["endgameScore"] = sum([t.Climb_Type_Park * 5 + t.Climb_Type_Park * 25 for t in teams if t != [] and t.teamid in alliance[color]])
            data[color]["climbTimeScore"] = sum([30 - t.Climb_Time_avg for t in teams if t != [] and t.teamid in alliance[color] and t.Climb_Time_avg is not None])
        payload["data"] = data

    return payload

@app.route("/team/<id>")
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
    payload["endgameScore"] = team.Climb_Type_Park
    payload["climbTimeScore"] = team.Climb_Time_avg

    matches = data_accessor.get_match_data(occured=False)
    team_matches = data_accessor.get_alliance(teamid=teamid)["matchid"].tolist()
    next_match = matches[(matches["matchId"].isin(team_matches)) & (matches["actual_time"] == datetime.datetime.fromtimestamp(0))].sort_values(by="predicted_time")["matchId"].iloc[0]
    payload["nextMatch"] = next_match
    payload["nextAlliance"] = data_accessor.get_alliance(teamid=teamid, match_key=next_match, type_df=False)[0].color
    return payload


@app.route("/match/<key>/prediction", methods=["POST"])
def match_prediction(key):
    if not key.startswith(f"{config.year}{config.event}"):
        key = f"{config.year}{config.event}_{key}"

    match = data_accessor.get_match_data(match_key=key, type_df=True,occured=False).loc[0]

    if match["actual_time"] != datetime.datetime.fromtimestamp(0):
        return "Match has already occurred"
    else:
        data = request.json
        data_accessor.add_prediction(data["scout"], key, data["prediction"])
        return "Prediction submitted"


@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/scouts/scouts")
def scout_names():
    return {"names":data_accessor.get_scouts()["id"].to_list()}

@app.route("/scouts/change", methods=["POST"])
def change_scouts():
    data = request.json
    if len(data["add"]) > 0:
        print(f"adding {data['add']}")
        data_accessor.add_scout(data["add"])
    if len(data["remove"]) > 0:
        data_accessor.delete_scout(data["remove"])
    return ""


@app.route("/scouts/data", methods=["GET", "POST"])
def scout_data():
    if (col:=request.args.get("sortBy")):
        return data_accessor.get_scouts().sort_values(by=col,axis=0,ascending=False).to_html(index=False)
    else:
        return data_accessor.get_scouts().to_html(index=False)

@app.route("/scouts", methods=["GET", "POST"])
def scouts():
    return render_template("scouts.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5001")
