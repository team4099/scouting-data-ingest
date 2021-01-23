import os

import gspread
import pymysql
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json

from DataInput import DataInput
from DataProcessor import DataProcessor
from terminal import console, logger
from rich.syntax import Syntax


class DataManager:
    def __init__(self):
        # Get logger
        self.log = logger

        self.log.info("[bold green]Starting [bold blue]Scouting-Data-Ingest")
        # Loading configuration
        self.log.info("[bold blue]Validating Configuration")
        with open("config/config.json") as f:
            config = json.load(f)

        self.config = config

        if self.validate_config() is False:
            self.log.critical("Quitting.")
            raise Exception
        else:
            self.log.info("[bold blue]Configuration Validated!")

        self.event = self.config["Event"]
        self.year = self.config["Year"]

        # Connecting to MySQL
        self.log.info("[bold blue]Connecting to MySQL")
        self.engine = create_engine(
            f'mysql+pymysql://{self.config["Database User"]}:{self.config["Database Password"]}@localhost/scouting'
        )
        self.Sessiontemplate = sessionmaker()
        self.Sessiontemplate.configure(bind=self.engine)
        self.session = self.Sessiontemplate()
        self.connection = self.engine.connect()

        self.log.info("[bold blue]Loading Components")
        self.dataInput = DataInput(self.engine, self.session, self.connection)
        self.dataProcessor = DataProcessor(self.engine, self.session, self.connection)

        self.log.info("[bold blue]Loaded Scouting-Data-Ingest!")

    def validate_config(self):
        if "TBA-Key" not in self.config:
            self.log.error(
                "You are missing the TBA-Key field. Please check https://github.com/team4099/scouting-data-ingest#tba for more information."
            )
            return False
        elif (
            requests.get(
                "https://www.thebluealliance.com/api/v3/status",
                headers={"X-TBA-Auth-Key": self.config["TBA-Key"]},
            ).status_code
            == 401
        ):
            self.log.error(
                "The key listed in the TBA-Key field is not valid. Please ensure the key is correct."
            )
            return False

        if "Year" not in self.config:
            self.log.error(
                "You are missing the Year field. Please add one in the style shown below."
            )
            year_example = """
            {
                "Year": "2020"
            }
            """
            console.print(Syntax(year_example, "json"))
            console.print(
                "Reference https://github.com/team4099/scouting-data-ingest#configuration for more information."
            )
            return False

        if "Google-Credentials" not in self.config:
            self.log.error(
                "You are missing the Google-Credentials field. Please check https://github.com/team4099/scouting-data-ingest#google-service-account-credentials-file for more information."
            )
            return False
        elif not os.path.isfile(f'config/{self.config["Google-Credentials"]}'):
            self.log.error(
                "The file listed in the Google-Credentials field does not exist in the config folder. Please place it inside the config folder."
            )
            return False
        else:
            try:
                gc = gspread.service_account(
                    f'./config/{self.config["Google-Credentials"]}'
                )
            except ValueError as e:
                self.log.error(
                    "The file listed in the Google-Credentials Field is improper. See below for details."
                )
                self.log.error(e)
                return False

        if "Spreadsheet" not in self.config:
            self.log.error(
                "You are missing the Spreadsheet field. Please check https://github.com/team4099/scouting-data-ingest#spreadsheet for more information."
            )
            return False
        else:
            try:
                sheet = gc.open(f'{self.config["Spreadsheet"]}').get_worksheet(0)
            except gspread.exceptions.SpreadsheetNotFound:
                self.log.error(
                    "The file listed in the Spreadsheets field has not been shared with the service account. Please make sure it is."
                )
                return False

        if "Database User" not in self.config:
            self.log.error(
                "You are missing the Database User field. Please check https://github.com/team4099/scouting-data-ingest#mysql for more information."
            )
            return False

        if "Database Password" not in self.config:
            self.log.error(
                "You are missing the Database Password field. Please check https://github.com/team4099/scouting-data-ingest#mysql for more information."
            )
            return False

        try:
            engine = create_engine(
                f'mysql+pymysql://{self.config["Database User"]}:{self.config["Database Password"]}@localhost/scouting'
            )
        except pymysql.err.OpertionalError:
            self.log.error(
                "Your Databse user name and/or password is not correct. Please verify them."
            )

        if "Event" not in self.config:
            self.log.error(
                "You are missing the Event field. Please check https://github.com/team4099/scouting-data-ingest#event for more information."
            )
            return False

        if (
            requests.get(
                f"https://www.thebluealliance.com/api/v3/event/{self.config['Year']}{self.config['Event']}",
                headers={"X-TBA-Auth-Key": self.config["TBA-Key"]},
            ).status_code
            == 404
        ):
            self.log.error(
                "The event listed in the TBA-Key field is not valid. Please ensure the event key and year are correct."
            )
            return False

        return True

    def get_data(self):
        self.log.info(f"[bold blue]Getting data for {self.year + self.event}")
        self.dataInput.getTBAData(self.year + self.event)
        self.dataInput.getSheetData(self.year + self.event)

    def check_data(self):
        return self.dataProcessor.checkData()

    def refresh(self):
        self.get_data()
        warnings = self.check_data()
        return warnings
