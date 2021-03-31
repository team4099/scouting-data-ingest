import json
import os

import gspread
import pymysql
import requests
import urllib3
from rich.syntax import Syntax
from sqlalchemy import create_engine

from terminal import console


class Config:

    def __init__(self, logger, simulation):
        self.config_dict = None
        self.log = logger
        self.simulation = simulation

        self.tba_key = None
        self.year = None
        self.google_credentials = None
        self.spreadsheet = None
        self.simulator_spreadsheet = None
        self.simulator_url = None
        self.db_user = None
        self.db_pwd = None
        self.event = None

        self.refresh()

    def refresh(self, validate=False):
        with open("config/config.json") as f:
            config = json.load(f)

        self.config_dict = config

        self.tba_key = config.get("TBA-Key")
        self.year = config.get("Year")
        self.google_credentials = config.get("Google-Credentials")
        self.spreadsheet = config.get("Spreadsheet")
        self.simulator_spreadsheet = config.get("Simulator Spreadsheet")
        self.simulator_url = config.get("Simulator URL")
        self.db_user = config.get("Database User")
        self.db_pwd = config.get("Database Password")
        self.event = config.get("Event")

        if validate:
            return self.validate()
        else:
            return None

    def validate(self):
        """

        Runs validation on the configuration.

        :return: Whether the configuration is valid
        :rtype: bool
        """
        if self.tba_key is None:
            self.log.error(
                "You are missing the TBA-Key field. Please check https://github.com/team4099/scouting-data-ingest#tba for more information."
            )
            return False
        elif requests.get("https://google.com").status_code == 401:
            self.log.error(
                "The key listed in the TBA-Key field is not valid. Please ensure the key is correct."
            )
            return False

        if self.year is None:
            self.log.error("You are missing the Year field. Please add one in the style shown below.")
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

        if self.google_credentials is None:
            self.log.error(
                "You are missing the Google-Credentials field. Please check https://github.com/team4099/scouting-data-ingest#google-service-account-credentials-file for more information."
            )
            return False
        elif not os.path.isfile(f'config/{self.google_credentials}'):
            self.log.error(
                "The file listed in the Google-Credentials field does not exist in the config folder. Please place it inside the config folder."
            )
            return False
        else:
            try:
                gc = gspread.service_account(
                    f'./config/{self.google_credentials}'
                )
            except ValueError as e:
                self.log.error(
                    "The file listed in the Google-Credentials Field is improper. See below for details."
                )
                self.log.error(e)
                return False

        if self.spreadsheet is None:
            self.log.error(
                "You are missing the Spreadsheet field. Please check https://github.com/team4099/scouting-data-ingest#spreadsheet for more information."
            )
            return False
        else:
            try:
                gc.open(f'{self.spreadsheet}').get_worksheet(0)
            except gspread.exceptions.SpreadsheetNotFound:
                self.log.error(
                    "The file listed in the Spreadsheets field has not been shared with the service account. Please make sure it is."
                )
                return False

        if self.db_user is None:
            self.log.error(
                "You are missing the Database User field. Please check https://github.com/team4099/scouting-data-ingest#mysql for more information."
            )
            return False

        if self.db_pwd is None:
            self.log.error(
                "You are missing the Database Password field. Please check https://github.com/team4099/scouting-data-ingest#mysql for more information."
            )
            return False

        try:
            # TODO: Fix URL for non self-hosted scenarios
            create_engine(
                f'mysql+pymysql://{self.db_user}:{self.db_pwd}@localhost/scouting'
            )
        except pymysql.err.OperationalError:
            self.log.error(
                "Your Database user name and/or password is not correct. Please verify them."
            )

        if self.event is None:
            self.log.error(
                "You are missing the Event field. Please check https://github.com/team4099/scouting-data-ingest#event for more information."
            )
            return False

        if requests.get(f"https://www.thebluealliance.com/api/v3/event/{self.year}{self.event}", headers={"X-TBA-Auth-Key": self.tba_key}).status_code == 404:
            self.log.error(
                "The event listed in the TBA-Key field is not valid. Please ensure the event key and year are correct."
            )
            return False

        if self.simulation:
            if self.simulator_url is None:
                self.log.error(
                    "You are missing the Simulator URL field. Please check https://github.com/team4099/scouting-data-ingest#tba for more information."
                )
                return False

            try:
                simulator_status = requests.get(f"{self.simulator_url}/matches").status_code
            except (ConnectionRefusedError, urllib3.exceptions.NewConnectionError, requests.exceptions.ConnectionError):
                self.log.error("The simulator may not be running or it's at a different url than the one provided.")
                return False

            if simulator_status == 401:
                self.log.error(
                    "The simulator may not be running. Please make sure it is and that it is up-to-date."
                )
                return False

            if self.simulator_spreadsheet is None:
                self.log.error(
                    "You are missing the Simulator Spreadsheet field. Please check https://github.com/team4099/scouting-data-ingest#spreadsheet for more information."
                )
                return False
            else:
                try:
                    gc.open(f'{self.simulator_spreadsheet}').get_worksheet(0)
                except gspread.exceptions.SpreadsheetNotFound:
                    self.log.error(
                        "The file listed in the Simulator Spreadsheet field has not been shared with the service account. Please make sure it is. Please also make sure the name entered is correct."
                    )
                    return False

        return True
