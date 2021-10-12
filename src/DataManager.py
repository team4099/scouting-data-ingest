import time
import os

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from requests import post, get

from Config import Config
from DataAccessor import DataAccessor
from DataCalculator import DataCalculator
from DataInput import DataInput
from DataProcessor import DataProcessor
from SQLObjects import Base


class DataManager:
    def __init__(self, skip_validation=False, interval=180, simulation=False):
        self.log = logger.opt(colors=True)

        self.log.info("Starting Scouting-Data-Ingest")
        # Loading configuration
        self.log.info("Validating Configuration")

        self.simulation = simulation
        self.config = Config(self.log, self.simulation)

        if not skip_validation:
            if self.config.validate() is False:
                self.log.critical("Quitting.")
                raise Exception
            else:
                self.log.info("Configuration Validated!")
        else:
            self.log.warning(
                "You have chosen to skip the configuration validation. Be aware that you may encounter errors."
            )

        # Connecting to MySQL
        self.log.info("Connecting to MySQL")
        self.engine = create_engine(
            f"mysql+pymysql://{self.config.db_user}:{self.config.db_pwd}@{self.config.db_host}/scouting"
        )
        self.session_template = sessionmaker()
        self.session_template.configure(bind=self.engine)
        self.session = self.session_template()
        self.connection = self.engine.connect()

        self.log.info("Erasing existing data")
        Base.metadata.drop_all(self.engine)
        self.session.commit()
        Base.metadata.create_all(self.engine)

        self.log.info("Loading Components")
        self.data_accessor = DataAccessor(
            self.engine, self.session, self.connection, self.config
        )
        self.data_input = DataInput(
            self.engine,
            self.session,
            self.connection,
            self.data_accessor,
            self.config,
        )
        self.data_processor = DataProcessor(self.data_accessor, self.config)
        self.data_calculator = DataCalculator(
            self.engine, self.session, self.connection, self.data_accessor, self.config
        )

        self.interval = interval
        self.data_accessor.add_info("Status", "Paused")
        self.data_accessor.add_info("Task", "Waiting")
        self.data_accessor.add_info("Last Match", "N/A")

        self.log.info("Loaded Scouting-Data-Ingest!")

    def get_data(self):
        """
        Gets Data from TBA and Google Sheets

        """
        self.data_accessor.update_info("Task", "Getting Data")
        self.log.info(f"Getting data for {self.config.year + self.config.event}")
        self.data_input.get_tba_data()
        self.data_input.get_sheet_data(self.config.year + self.config.event)
        self.session.commit()

    def check_data(self):
        """

        Checks the data for errors.

        """
        self.data_accessor.update_info("Task", "Checking Data")
        self.data_processor.check_data()

    def calculate_data(self):
        """
        Calculates TeamData
        """
        self.data_accessor.update_info("Task", "Performing Calculations on data")
        self.data_calculator.calculate_team_data()

    def refresh(self):
        """
        Gets Data and then checks it and calculates new data.

        """
        self.data_accessor.update_info("Status", "Running")
        self.get_data()
        self.check_data()
        self.calculate_data()
        self.data_accessor.update_info("Task", "Waiting")
        self.data_accessor.update_info("Status", "Finished")
        self.data_accessor.update_info("Last Match", self.data_input.last_tba_match)
        self.log.info("Run finished.")

    def start(self):
        """
        Starts the ingest
        """
        self.data_accessor.update_info("Status", "Running")
        start_time = time.time()
        while True:
            self.refresh()
            self.data_accessor.update_info("Task", "Waiting")
            time.sleep(self.interval - ((time.time() - start_time) % self.interval))
