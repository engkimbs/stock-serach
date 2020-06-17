import logging
from logging.handlers import RotatingFileHandler


class StockLog:
    _instance = None

    @classmethod
    def _getInstance(cls):
        return cls._instance

    @classmethod
    def instance(cls):
        cls._instance = cls()
        cls.instance = cls._getInstance
        return cls._instance

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        c_handler = logging.StreamHandler()
        # f_handler = logging.FileHandler('crawler.log')
        f_handler = RotatingFileHandler('crawler.log', encoding='utf-8', maxBytes=1048576, backupCount=1000)

        c_handler.setLevel(logging.DEBUG)
        f_handler.setLevel(logging.DEBUG)

        # Create formatters and add it to handlers
        c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        self.logger.addHandler(c_handler)
        self.logger.addHandler(f_handler)

    def info(self, msg):
        self.logger.info(msg)