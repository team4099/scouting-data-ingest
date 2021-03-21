import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json

from DataCalculator import DataCalculator
from DataInput import DataInput
from DataProcessor import DataProcessor
from DataAccessor import DataAccessor
from Config import Config
from loguru import logger


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
                "You have chosen to skip the configuration validation. Be aware that you may encounter errors.")

        # Connecting to MySQL
        self.log.info("Connecting to MySQL")
        self.engine = create_engine(
            f'mysql+pymysql://{self.config.db_user}:{self.config.db_pwd}@localhost/scouting'
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
            f"matchdata{self.config.year}",
            "`match`",
            "team_data",
            f"teamdata{self.config.year}",
            f"calculatedteamdata{self.config.year}",
            "team",
        ]
        for t in tables:
            tex = text(f"drop table if exists {t}")
            self.connection.execute(tex)
        self.session.commit()

        self.log.info("Loading Components")
        self.data_accessor = DataAccessor(self.engine, self.session, self.connection, self.config)
        self.data_input = DataInput(self.engine, self.session, self.connection, self.data_accessor, self.config)
        self.data_processor = DataProcessor(self.data_accessor, self.config)
        self.data_calculator = DataCalculator(self.engine, self.session, self.connection, self.data_accessor, self.config)

        self.interval = interval

        self.log.info("Loaded Scouting-Data-Ingest!")

    def get_data(self):
        """
        Gets Data from TBA and Google Sheets

        """
        self.log.info(f"Getting data for {self.config.year + self.config.event}")
        self.data_input.get_tba_data(self.config.year + self.config.event)
        self.data_input.get_sheet_data(self.config.year + self.config.event)
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
