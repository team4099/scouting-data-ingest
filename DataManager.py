import os
import sched
import time

import gspread
import pymysql
import requests
import urllib3
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json

from DataCalculator import DataCalculator
from DataInput import DataInput
from DataProcessor import DataProcessor
from DataAccessor import DataAccessor
from terminal import console
from rich.syntax import Syntax
from loguru import logger


class DataManager:
    def __init__(self, skip_validation=False, interval=180, simulation=False):
        self.log = logger.opt(colors=True).bind(color="<green>")

        self.log.info("Starting Scouting-Data-Ingest")
        # Loading configuration
        self.log.info("Validating Configuration")
        with open("config/config.json") as f:
            config = json.load(f)

        self.config = config
        self.simulation = simulation

        if not skip_validation:
            if self.validate_config() is False:
                self.log.critical("Quitting.")
                raise Exception
            else:
                self.log.info("Configuration Validated!")
        else:
            self.log.warning(
                "You have chosen to skip the configuration validation. Be aware that you may encounter errors.")

        self.event = self.config["Event"]
        self.year = self.config["Year"]

        # Connecting to MySQL
        self.log.info("Connecting to MySQL")
        self.engine = create_engine(
            f'mysql+pymysql://{self.config["Database User"]}:{self.config["Database Password"]}@localhost/scouting'
        )
        self.session_template = sessionmaker()
        self.session_template.configure(bind=self.engine)
        self.session = self.session_template()
        self.connection = self.engine.connect()

        self.log.info("Erasing existing data")
        tables = [
            'red_association',
            'blue_association',
            "match_data",
            f"matchdata{self.config['Year']}",
            "`match`",
            "team_data",
            f"teamdata{self.config['Year']}",
            f"calculatedteamdata{self.config['Year']}",
            "team",
        ]
        for t in tables:
            tex = text(f"drop table if exists {t}")
            self.connection.execute(tex)
        self.session.commit()

        self.log.info("Loading Components")
        self.data_accessor = DataAccessor(self.engine, self.session, self.connection)
        self.data_input = DataInput(self.engine, self.session, self.connection, self.data_accessor, simulation)
        self.data_processor = DataProcessor(self.data_accessor)
        self.data_calculator = DataCalculator(self.engine, self.session, self.connection, self.data_accessor)

        self.interval = interval

        self.log.info("Loaded Scouting-Data-Ingest!")

    def validate_config(self):
        """

        Runs validation on the configuration.

        :return: Whether the configuration is valid
        :rtype: bool
        """
        if "TBA-Key" not in self.config:
            self.log.error(
                "You are missing the TBA-Key field. Please check https://github.com/team4099/scouting-data-ingest#tba for more information."
            )
            return False
        elif (
                requests.get(
                    "https://google.com"
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
                gc.open(f'{self.config["Spreadsheet"]}').get_worksheet(0)
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
            create_engine(
                f'mysql+pymysql://{self.config["Database User"]}:{self.config["Database Password"]}@localhost/scouting'
            )
        except pymysql.err.OperationalError:
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

        if self.simulation and "Simulator URL" not in self.config:
            self.log.error(
                "You are missing the Simulator URL field. Please check https://github.com/team4099/scouting-data-ingest#tba for more information."
            )
            return False
        try:
            simulator_status = requests.get(f"{self.config['Simulator URL']}/matches").status_code
        except ConnectionRefusedError or urllib3.exceptions.NewConnectionError or requests.exceptions.ConnectionError:
            self.log.error("The simulator may not be running or it's at a different url than the one provided.")
            return False

        if simulator_status == 401:
            self.log.error(
                "The simulator may not be running. Please make sure it is and that it is up-to-date."
            )
            return False

        if self.simulation and "Simulator Spreadsheet" not in self.config:
            self.log.error(
                "You are missing the Simulator Spreadsheet field. Please check https://github.com/team4099/scouting-data-ingest#spreadsheet for more information."
            )
            return False
        else:
            try:
                gc.open(f'{self.config["Simulator Spreadsheet"]}').get_worksheet(0)
            except gspread.exceptions.SpreadsheetNotFound:
                self.log.error(
                    "The file listed in the Simulator Spreadsheet field has not been shared with the service account. Please make sure it is. Please also make sure the name entered is correct."
                )
                return False

        return True

    def get_data(self):
        """
        Gets Data from TBA and Google Sheets

        """
        self.log.info(f"Getting data for {self.year + self.event}")
        self.data_input.get_tba_data(self.year + self.event)
        self.data_input.get_sheet_data(self.year + self.event)
        self.session.commit()

    def check_data(self):
        """

        Checks the data for errors.

        :return: A dict of warnings
        :rtype: Dict[str, List]
        """
        return self.data_processor.check_data()

    def calculate_data(self):
        """
        Calculates TeamData
        """
        self.data_calculator.calculate_team_data()

    def refresh(self):
        """
        Gets Data and then checks it and calculates new data.

        :return: A dictionary of warnings
        :rtype: Dict[str, List]
        """
        self.get_data()
        warnings = self.check_data()
        self.calculate_data()
        self.log.info("Run finished.")
        return warnings

    def start(self):
        """
        Starts the ingest
        """
        start_time = time.time()
        while True:
            self.refresh()
            time.sleep(self.interval - ((time.time() - start_time) % self.interval))
