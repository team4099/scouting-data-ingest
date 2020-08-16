import numpy as np
import pandas as pd
from gspread_pandas import Spread, Client
from sqlalchemy import create_engine
import pymysql
import tbaapiv3client
import dotenv
import os
from flask import Flask, request

dotenv.load_dotenv()
app = Flask(__name__)
sql_engine = create_engine('mysql+pymysql://root:reee@127.0.0.1/scouting', pool_recycle=3600)
db_connection = sql_engine.connect()


EVENT = os.getenv("EVENT")
TBA_API_KEY = os.getenv("TBA_API_KEY")


@app.route('/')
def hello_world():
    return 'Hello, World!'


@app.route('/load_data', methods=['POST'])
def load_data():
    spread = Spread(request.json["document"])
    df = spread.sheet_to_df(sheet=spread.sheets[0])
    df.index = pd.to_datetime(df.index)
    # del df['Timestamp']
    df.to_sql("data", db_connection, if_exists="replace", index=True, index_label='id')
    return 'success!'


if __name__ == "__main__":
    app.run()
