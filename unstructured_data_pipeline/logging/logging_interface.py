"""
logging_interface
~~~~~~~~~~~~~~~~~
"""
import abc

# Logger Interface #
####################################################################################################
class PipelineLoggerInterface(metaclass=abc.ABCMeta):
    """A logs interface for the data pipeline logs classes"""

    @abc.abstractmethod
    def config(self, *args, **kwargs):
        """logger configuration"""
        raise NotImplementedError

    @abc.abstractmethod
    def set_logfile_path(self, file_path: str, file_name: str):
        """update the path to the log file."""
        raise NotImplementedError

    @abc.abstractmethod
    def debug(self, **kwargs):
        """logs.debug method"""
        raise NotImplementedError

    @abc.abstractmethod
    def info(self, **kwargs):
        """logs.info method"""
        raise NotImplementedError

    @abc.abstractmethod
    def warning(self, **kwargs):
        """logs.warning method"""
        raise NotImplementedError

    @abc.abstractmethod
    def error(self, **kwargs):
        """logs.error method"""
        raise NotImplementedError

    @abc.abstractmethod
    def critical(self, **kwargs):
        """logs.critical method"""
        raise NotImplementedError