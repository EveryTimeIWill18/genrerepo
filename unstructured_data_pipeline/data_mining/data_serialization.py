"""
data_serialization
~~~~~~~~~~~~~~~~~~
This module is used to prepare data for sftp transportation.
"""
import os
import pickle
from configparser import ConfigParser
from unstructured_data_pipeline.config_path import config_file_path
from unstructured_data_pipeline.data_mining.data_parsers import d
from unstructured_data_pipeline.logging.logging_config import BaseLogger
from unstructured_data_pipeline.config_path import config_file_path

# setup logging configuration
config = ConfigParser()
config.read(config_file_path)
sections = config.sections()
logfile = "dms_unst_pipeline_log_" + d
logger = BaseLogger(config.get(sections[2], 'DMS_LOGGING'), logfile)
logger.config() # use the default logging configuration

class DataSerialization(object):
    """
    Securely loads the correct local and remote data paths
    to be used during sftp file transfer.
    """
    def __init__(self):
        self.data_serialized: bool = False

    def __get_local_pickle_path(self) -> str:
        config = ConfigParser()
        config.read(config_file_path)
        sections = config.sections()
        local_pickle_path = config.get(sections[1], 'DMS_PICKLE')
        return local_pickle_path

    def get_local_pickle_path(self) -> str:
        return self.__get_local_pickle_path()

    def __get_remote_pickle_path(self) -> str:
        config = ConfigParser()
        config.read(config_file_path)
        sections = config.sections()
        remote_pickle_path = config.get(sections[3], 'REMOTE_PICKLE_PATH')
        return remote_pickle_path

    def get_remote_pickle_path(self) -> str:
        return self.__get_remote_pickle_path()

    def __get_local_mapping_file_path(self) -> str:
        config = ConfigParser()
        config.read(config_file_path)
        sections = config.sections()
        local_mapping_file_path = config.get(sections[2], 'DMS_MAPPING_PATH')
        return local_mapping_file_path

    def get_local_mapping_file_path(self) -> str:
        return self.__get_local_mapping_file_path()

    def __get_remote_mapping_file_path(self) -> str:
        config = ConfigParser()
        config.read(config_file_path)
        sections = config.sections()
        remote_mapping_file_path = config.get(sections[3], 'REMOTE_MAPPING_PATH')
        return remote_mapping_file_path

    def get_remote_mapping_file_path(self) -> str:
        return self.__get_remote_mapping_file_path()


def serialize_data(data_set: any, write_name, is_local=True, is_pickle=True) -> None:
    """
    This function serializes a data set to be transported
    to a remote server via sftp.

    :param data_set:
    :return:
    """
    try:
        data_serialization = DataSerialization()
        if is_local:
            if is_pickle:
                # change directory to the local pickle directory
                os.chdir(data_serialization.get_local_pickle_path())
                file_name = write_name + '_' + d + '.pickle'
                try:
                    pickle.dump(data_set, open(file_name, 'wb'), protocol=2)
                except (pickle.PickleError, Exception) as e:
                    logger.error(error=e)
            else:
                # change directory to the local mapping file path
                os.chdir(data_serialization.get_local_mapping_file_path())
                file_name = write_name + '_' + d + '.pickle'
                try:
                    pickle.dump(data_set, open(file_name, 'wb'), protocol=2)
                except (pickle.PickleError, Exception) as e:
                    logger.error(error=e)
        else:
            if is_pickle:
                # change directory to the remote pickle directory
                os.chdir(data_serialization.get_remote_pickle_path())
                file_name = write_name + '_' + d + '.pickle'
                try:
                    pickle.dump(data_set, open(file_name, 'wb'), protocol=2)
                except (pickle.PickleError, Exception) as e:
                    logger.error(error=e)
            else:
                # change directory to the remote mapping file path
                os.chdir(data_serialization.get_remote_mapping_file_path())
                file_name = write_name + '_' + d + '.pickle'
                try:
                    pickle.dump(data_set, open(file_name, 'wb'))
                except (pickle.PickleError, Exception) as e:
                    logger.error(error=e)
    except Exception as e:
        logger.error(error=e)