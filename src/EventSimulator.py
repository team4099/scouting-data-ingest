import json
from datetime import datetime

import gspread
import pandas as pd
from flask import Flask, make_response, render_template, request
from pytz import timezone

app = Flask(__name__)
currMatch = 76
with open("./config/config.json") as f:
    config = json.load(f)
gc = gspread.service_account(f'./config/{config["Google-Credentials"]}')
sim_file = gc.open(f'{config["Simulator Spreadsheet"]}')

if "Data Worksheet" not in [i.title for i in sim_file.worksheets()]:
    # They've not run the "new" (as of 5/7/21) version so add it
    main_sheet = gc.open(f'{config["Spreadsheet"]}').get_worksheet(0)
    main_data = pd.DataFrame(main_sheet.get_all_records())
    new_sheet = sim_file.add_worksheet(
        "Data Worksheet", rows=main_sheet.row_count, cols=main_sheet.col_count
    )
    new_sheet.update(
        "A1", [main_data.columns.values.tolist()] + main_data.values.tolist()
    )
    new_sheet.freeze(rows=1)

orig_sheet = sim_file.worksheet("Data Worksheet")
orig_data = pd.DataFrame(orig_sheet.get_all_records())
sim_sheet = gc.open(f'{config["Simulator Spreadsheet"]}').get_worksheet(0)

with open("./data/2020vahay.json") as f:
    data = f.read()
    all_matches = json.loads(data)
    max_matches = len(all_matches)
    not_played_matches = json.loads(data)
    for match in not_played_matches:
        match["alliances"]["red"]["score"] = 0
        match["alliances"]["blue"]["score"] = 0
        match["winning_alliance"] = None
        match["time"] = 0
        match["post_result_time"] = 0
        match["actual_time"] = 0
        match["score_breakdown"] = {}


lastModified = datetime.now(tz=timezone("GMT"))


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
    if int(request.args["match_num"]) < currMatch:
        return render_template(
            "eventsim.html",
            count=currMatch,
            max_matches=max_matches,
            warning="Next match must be higher. Restart if you want to see what happens at a lower match number.",
        )
    currMatch = int(request.args["match_num"])
    updateSheet(currMatch)
    global lastModified
    lastModified = datetime.now(tz=timezone("GMT"))
    return render_template("eventsim.html", count=currMatch, max_matches=max_matches)


@app.route("/")
def index():
    return render_template("eventsim.html", count=currMatch, max_matches=max_matches)


if __name__ == "__main__":
    updateSheet(currMatch)
    app.run(host="0.0.0.0")
