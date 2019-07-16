"""
logging_config
~~~~~~~~~~~~~
"""
import os
import logging
from unstructured_data_pipeline.logging.logging_interface import PipelineLoggerInterface


# Concrete Logger Class #
####################################################################################################
class BaseLogger(PipelineLoggerInterface):
    """Parent logger class for each file type"""

    def __init__(self, log_file_path: str, log_file_name: str):
        self.logger = logging.getLogger(__name__)
        self.log_file_path = log_file_path
        self.log_file_name = log_file_name + '.log'
        self.file_handler: logging.FileHandler = None

    def config(self, *args, **kwargs):
        """logger configuration"""

        # set logs file format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
        # set logs file handler
        self.file_handler = logging.FileHandler(os.path.join(self.log_file_path, self.log_file_name))

        # add to the current logger
        self.logger.addHandler(self.file_handler)
        self.file_handler.setFormatter(formatter)

        # set the logs level
        self.logger.setLevel(logging.INFO)

    def set_logfile_path(self, file_path: str, file_name: str):
        self.log_file_path = file_path
        self.log_file_name = file_name

    def debug(self, **kwargs):
        """logs.debug method"""
        self.file_handler.setLevel(logging.DEBUG)
        self.logger.debug(f"\nDebug: {kwargs.get('debug')}")

    def info(self, **kwargs):
        """logs.info method"""
        self.file_handler.setLevel(logging.INFO)
        self.logger.info(f"\nInfo: {kwargs.get('info')}")

    def warning(self, **kwargs):
        """logs.warning method"""
        self.file_handler.setLevel(logging.ERROR)
        self.logger.error(f"\nWarning: {kwargs.get('error')}")

    def error(self, **kwargs):
        """logs.error method"""
        self.file_handler.setLevel(logging.ERROR)
        self.logger.error(f"\nError: {kwargs.get('error')}")

    def critical(self, **kwargs):
        """logs.critical method"""
        self.file_handler.setLevel(logging.CRITICAL)
        self.logger.critical(f"\nCritical: {kwargs.get('critical')}")