from copy import copy, deepcopy
import json
from datetime import datetime

import pandas as pd
from flask import Flask, make_response, render_template, request
from pytz import timezone
import pytz
from Config import Config
from loguru import logger
import requests
import sys

app = Flask(__name__)
lastMatch = 0
currMatch = 0
config = Config(logger, simulation=True)

with open("./data/2022week0.json") as f:
    data = f.read()
    all_matches = json.loads(data)
    all_matches = sorted(all_matches,key=lambda x: x["post_result_time"])
    max_matches = len(all_matches)
    not_played_matches = deepcopy(all_matches)
    for match in not_played_matches:
        match["alliances"]["red"]["score"] = 0
        match["alliances"]["blue"]["score"] = 0
        match["winning_alliance"] = None
        match["time"] = 0
        match["post_result_time"] = 0
        match["actual_time"] = 0
        match["score_breakdown"] = {}

with open("./data/gen_data.json") as f:
    team_datum = json.loads(f.read())

lastModified = pytz.timezone("GMT").localize(datetime.fromtimestamp(all_matches[currMatch]["post_result_time"] - 1), is_dst=None)


@app.route("/matches")
def matchHandler():
    curr_matches = all_matches[:currMatch]
    next_matches = not_played_matches[currMatch:]
    curr_matches.extend(next_matches)

    if request.headers.get("If-Modified-Since") != None and datetime.strptime(
        request.headers.get("If-Modified-Since"), "%a, %d %b %Y %H:%M:%S %Z"
    ).replace(tzinfo=timezone("GMT")) >= lastModified.replace(microsecond=0):
        x = make_response()
        x.status_code = 304
        return x
    x = make_response(json.dumps(curr_matches))
    x.headers["Last-Modified"] = lastModified.strftime("%a, %d %b %Y %H:%M:%S %Z")
    return x


@app.route("/teams")
def teamHandler():
    team_list = set()
    for match in all_matches:
        team_list.update(match["alliances"]["red"]["team_keys"])
        team_list.update(match["alliances"]["blue"]["team_keys"])
    team_list = list(team_list)
    x = make_response(json.dumps(team_list))
    return x


def updateSheet(match):
    orig_sheet = sim_file.worksheet("Data Worksheet")
    updated_data = pd.DataFrame(orig_sheet.get_all_records())
    match_dataframe = updated_data[
        updated_data["Match Key"].apply(lambda x: int(x.lstrip("qm"))) <= match
    ]
    global sim_sheet
    sim_sheet.resize(rows=1)
    sim_sheet.resize(rows=1000)
    sim_sheet.update(
        "A1",
        [match_dataframe.columns.values.tolist()] + match_dataframe.values.tolist(),
    )
    return None
    
@app.route("/form")
def form():
    global currMatch
    global lastMatch
    if int(request.args["match_num"]) < currMatch:
        return render_template(
            "eventsim.html",
            count=currMatch,
            max_matches=max_matches,
            warning="Next match must be higher. Restart if you want to see what happens at a lower match number.",
        )
    currMatch = int(request.args["match_num"])
    for match in range(lastMatch, currMatch):
        for team_data in team_datum["2022week0_qm" + str(match + 1)]:
            requests.post("http://localhost:5001/api/add_team_datum", json=team_data)
    lastMatch = currMatch
    global lastModified
    lastModified = pytz.timezone("GMT").localize(datetime.fromtimestamp(all_matches[lastMatch]["post_result_time"]), is_dst=None)
    return render_template("eventsim.html", count=currMatch, max_matches=max_matches)


@app.route("/")
def index():
    return render_template("eventsim.html", count=currMatch, max_matches=max_matches)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
