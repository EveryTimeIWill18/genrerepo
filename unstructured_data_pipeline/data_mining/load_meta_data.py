"""
load_meta_data
~~~~~~~~~~~~~~
"""
import os
import csv
import stat
import xlrd
import time
import pickle
import pandas as pd
from datetime import datetime, timedelta

class StorageStruct(object):
    memory_store = list()
    SIZE = 0

    def load_data(*args):
        """update the memory store list"""
        temp_list = [a for a in args]
        __class__.memory_store.append(temp_list)
        for i, _ in enumerate(temp_list):
            __class__.SIZE += 1


class ProdDeltaStruct:
    LOADED_FILES: list = []

    @classmethod
    def insert_file(cls, file_name: str):
        cls.LOADED_FILES.append(file_name)

class ProdHistStruct:
    LOADED_FILES: list = []

    @classmethod
    def insert_file(cls, file_name: str):
        cls.LOADED_FILES.append(file_name)



class MetaDataBase(object):
    """
    MetaDataBase
    ~~~~~~~~~~~~
    Base class for the dev and historical metadata classes.
    """
    # meta data logging base path
    BASE_LOG = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
    HIST_LOG = r'historical'
    DELTA_LOG = r'delta'

    # log files for the different file dir types
    DEV_LOGFILE = r'dev_metadata_log.txt'
    PROD_LOGFILE = r'prod_metadata_log.txt'

    # data paths
    PROD = r'V:\Prod'
    DEV = r'V:\Dev'
    DELTA = r'Delta'
    HISTORICAL = r'Historical'

    @classmethod
    def get_file_stats(cls, cls_mthd: callable):
        file_stat_obj = os.stat(cls_mthd)
        modification_time = time.ctime(file_stat_obj[stat.ST_MTIME])
        return modification_time

    @classmethod
    def get_base_log_path(cls):
        return cls.BASE_LOG

    @classmethod
    def get_prod_path(cls):
        return cls.PROD

    @classmethod
    def get_dev_path(cls):
        return cls.DEV

    @classmethod
    def get_prod_hist_path(cls):
        return os.path.join(cls.PROD, cls.HISTORICAL)

    @classmethod
    def get_prod_delta_path(cls):
        return os.path.join(cls.PROD, cls.DELTA)

    @classmethod
    def get_dev_hist_path(cls):
        return os.path.join(cls.DEV, cls.HISTORICAL)

    @classmethod
    def get_dev_delta_path(cls):
        return os.path.join(cls.DEV, cls.DELTA)

    @classmethod
    def get_hist_log_path(cls):
        return os.path.join(cls.BASE_LOG, cls.HIST_LOG)

    @classmethod
    def get_delta_dev_log_file(cls):
        return os.path.join(cls.BASE_LOG, cls.DELTA_LOG, cls.DEV_LOGFILE)

    @classmethod
    def get_delta_prod_log_file(cls):
        return os.path.join(cls.BASE_LOG, cls.DELTA_LOG, cls.PROD_LOGFILE)

    @classmethod
    def get_hist_dev_log_file(cls):
        return os.path.join(cls.BASE_LOG, cls.HIST_LOG, cls.DEV_LOGFILE)

    @classmethod
    def get_hist_prod_log_file(cls):
        return os.path.join(cls.BASE_LOG, cls.HIST_LOG, cls.PROD_LOGFILE)



class MetaDataDev(MetaDataBase):
    """
    MetaDataDev
    ~~~~~~~~~~~
    """
    def __init__(self, is_historical: bool):
        super().__init__()
        self.delta = self.get_dev_delta_path()
        self.hist = self.get_dev_hist_path()
        self.is_historical = is_historical
        self.data_list: list = []
        self.previously_loaded_delta_data: list = []
        self.previously_loaded_hist_data: list = []
        self.delta_data_pointer: int = 0
        self.hist_data_pointer: int = 0

    def write_to_pickle(self):
        """
        Write the loaded metadata list to a pickle file.
        :return:
        """
        try:
            if self.is_historical:
                os.chdir(os.path.join(self.get_base_log_path(), 'historical'))
                print(f'Current Directory: {os.getcwd()}')
                with open(r'hist_metadata.pickle', 'wb') as f:
                    pickle.dump(self.previously_loaded_hist_data, f)
            else:
                os.chdir(os.path.join(self.get_base_log_path(), 'delta'))
                print(f'Current Directory: {os.getcwd()}')
                with open(r'dev_metadata.pickle', 'wb') as f:
                    pickle.dump(self.previously_loaded_delta_data, f)
        except (Exception, pickle.PicklingError) as e:
            print(e)

    def load_pickle(self):
        """
        Load the pickled meta data file.
        :return:
        """
        try:
            if self.is_historical:
                os.chdir(os.path.join(self.get_base_log_path(), 'historical'))
                with open(r'hist_metadata.pickle', 'rb') as f:
                    self.previously_loaded_hist_data = pickle.load(f)
            else:
                os.chdir(os.path.join(self.get_base_log_path(), 'delta'))
                with open(r'dev_metadata.pickle', 'wb') as f:
                    self.previously_loaded_delta_data = pickle.load(f)
        except (Exception, pickle.UnpicklingError) as e:
            print(e)

    def load_meta_data(self):
        """
        Load in the meta data excel file.
        :return:
        """
        try:
            # step 1: load the pickled list
            self.load_pickle()
            if self.is_historical:
                historical_files = os.listdir(self.get_dev_hist_path())
                print(historical_files)
            else:
                delta_files = os.listdir(self.get_dev_delta_path())
                print(delta_files)
                # case 1: no files to load
                if len(delta_files) == 0:
                    print(f"There's no data to load in Dev > Delta, directory")

                # case 2: path contains files but the list is empty
                if len(delta_files) > 0  and len(self.previously_loaded_delta_data) == 0:
                    delta_file = os.path.join(self.get_dev_delta_path(), delta_files[self.delta_data_pointer])
                    print(f"delta_file = {delta_file}")
                    meta_data_xls_file = os.path.join(delta_file, 'Metadata', delta_files[self.delta_data_pointer] + '.xls')
                    # write to a pandas DataFrame
                    self.metadata_df = pd.read_excel(
                        xlrd.open_workbook(
                            filename=meta_data_xls_file,
                            encoding_override='cp1252'
                        )
                    )
                    # update the list
                    self.previously_loaded_delta_data.append(
                        os.path.splitext(os.path.basename(delta_file))[self.delta_data_pointer]
                    )
                    # write to the pickle file
                    self.write_to_pickle()

                    # check the output
                    print(self.metadata_df.head())
                    self.delta_data_pointer += 1
        except Exception as e:
            print(e)




class MetaDataProd(MetaDataBase):
    """
    MetaDataHistorical
    ~~~~~~~~~~~~~~~~~~
    """

    def __init__(self, is_historical: bool):
        super().__init__()
        self.delta = self.get_prod_delta_path()
        self.hist = self.get_prod_hist_path()
        self.is_historical = is_historical
        self.data_list: list = []
        self.previously_loaded_delta_data: list = []
        self.previously_loaded_hist_data: list = []
        self.metadata_df: pd.DataFrame = None
        self.prod_delta_struct = ProdDeltaStruct()
        self.prod_hist_struct = ProdHistStruct()

    def write_to_metadata_log(self, delimiter: str, file_name: str):
        """
        Update the correct metadata log file.
        :return:
        """
        try:
            if self.is_historical:
                # step 1: open the metadata log file using a context manager
                with open(self.get_hist_prod_log_file(), 'a', newline='\n') as f:
                    # step 1.1: write to the log file, the current meta data log file that's been loaded
                    metadata_writer = csv.writer(
                        f, delimiter=delimiter,
                        quoting=csv.QUOTE_MINIMAL
                    )
                    # step 1.2: write the filename to the metadata log file
                    metadata_writer.writerow(os.path.splitext(os.path.basename(file_name))[0])
            else:
                # if data is delta
                with open(self.get_delta_prod_log_file(), 'a', newline='\n') as f:
                    metadata_writer = csv.writer(
                        f, delimiter=delimiter,
                        quoting=csv.QUOTE_MINIMAL
                    )
                    # step 1.2: write the filename to the metadata log file
                    metadata_writer.writerow(os.path.splitext(os.path.basename(file_name))[0])
        except Exception as e:
            print(e)

    def load_metadata_log(self):
        """
        Load the metadata log file.
        :return:
        """
        try:
            if self.is_historical:
                with open(self.get_hist_prod_log_file(), 'r') as f:
                    if len(f.read()) == 0:
                        print('No files read yet')
                    else:
                        f_empty = False
                        for row in f.read():
                            print(f'row: {row}')
                            self.previously_loaded_hist_data.append(row)
                        return f.read()
            else:
                with open(self.get_delta_prod_log_file(), 'r') as f:
                    if len(f.read()) == 0:
                        print('No files read yet')
                    else:
                        f_empty = False
                        for row in f.read():
                            print(f'row: {row}')
                            self.previously_loaded_delta_data.append(row)
                        return f.read()
        except Exception as e:
            print(e)

    def load_metadata(self):
        """
        Load a metadata logfile that has not been previously loaded.
        :return:
        """
        try:
            # PROD HISTORICAL PROCESS
            if self.is_historical:
                # step 1: load the prod historical log data
                current_list = self.load_metadata_log()
                # step 2: load the historical files
                print(f"in load_metadata")
                historical_files = os.listdir(self.get_prod_hist_path())
                print(f'len(historical_files): {len(historical_files)}')
                # step 2.1: check if the historical files directory is empty
                if len(historical_files) == 0:
                    # case 1: no files exist in: Prod > Historical directory
                    print(f"The directory, {self.get_prod_hist_path()} is empty.")
                    return f"The directory, {self.get_prod_hist_path()} is empty."

                elif len(self.previously_loaded_hist_data) == 0 and len(historical_files) > 0:
                    # case 2: metadata log file is empty but there are directories in historical
                    hist_file = os.path.join(self.get_prod_hist_path(), historical_files[0])
                    meta_data_xls_file = os.path.join(hist_file, 'Metadata', historical_files[0] + '.xls')
                    # write to a pandas DataFrame
                    self.metadata_df = pd.read_excel(
                        xlrd.open_workbook(
                            filename=meta_data_xls_file,
                            encoding_override='cp1252'
                        )
                    )
                    # update the logfile
                    self.write_to_metadata_log(delimiter='\n', file_name=historical_files[0])
                    print(f"Loaded file: {historical_files[0]} into a pandas DataFrame")
                    print(self.metadata_df.head())

                elif len(self.previously_loaded_hist_data) > 0 and len(historical_files) > 0:
                    # case 3: neither metadata log nor historical directory is empty
                    # get the files that have not been loaded
                    unloaded_hist_dirs = list(set(historical_files) - set(self.previously_loaded_hist_data))
                    hist_file = os.path.join(self.get_prod_hist_path(), unloaded_hist_dirs[0])
                    meta_data_xls_file = os.path.join(hist_file, 'Metadata', unloaded_hist_dirs[0] + '.xls')
                    # write to a pandas DataFrame
                    self.metadata_df = pd.read_excel(
                        xlrd.open_workbook(
                            filename=meta_data_xls_file,
                            encoding_override='cp1252'
                        )
                    )
                    # update the logfile
                    self.write_to_metadata_log(delimiter='\n', file_name=unloaded_hist_dirs[0])
                    print(f"Loaded file: {historical_files[0]} into a pandas DataFrame")
                    print(self.metadata_df.head())

            # PROD DELTA PROCESS
            else:
                MetaDataProd.load_metadata_log(self)

                delta_files = os.listdir(self.get_prod_delta_path())
                if len(delta_files) == 0:
                    # case 1: no files exist in: Prod > Delta directory
                    print(f"The directory, {self.get_prod_delta_path()} is empty.")
                    return f"The directory, {self.get_prod_delta_path()} is empty."

                elif len(self.previously_loaded_delta_data) == 0 and len(delta_files) > 0:
                    # case 2: metadata log file is empty but there are directories in historical
                    print(f"In case 2")
                    pos = StorageStruct.SIZE
                    StorageStruct.load_data(delta_files[pos])
                    print(StorageStruct.memory_store)
                    print(StorageStruct.SIZE)

                    delta_file = os.path.join(self.get_prod_delta_path(), delta_files[pos])
                    meta_data_xls_file = os.path.join(delta_file, 'Metadata', delta_files[pos] + '.xls')
                    # write to a pandas DataFrame
                    self.metadata_df = pd.read_excel(
                        xlrd.open_workbook(
                            filename=meta_data_xls_file,
                            encoding_override='cp1252'
                        )
                    )
                    # update the logfile
                    self.write_to_metadata_log(delimiter='\n', file_name=delta_files[0])
                    print(f"Loaded file: {delta_files[0]} into a pandas DataFrame")
                    print(self.metadata_df.head())

                elif len(self.previously_loaded_delta_data) > 0 and len(delta_files) > 0:
                    # case 3: neither metadata log nor delta directory is empty
                    # get the files that have not been loaded
                    print(f"Case 3: length previously_loaded_delta_data: {len(self.previously_loaded_delta_data)}")
                    unloaded_delta_dirs = list(set(delta_files) - set(self.previously_loaded_delta_data))
                    print(unloaded_delta_dirs)
                    if unloaded_delta_dirs[0] not in self.prod_delta_struct.LOADED_FILES:
                        self.prod_delta_struct.insert_file(file_name=unloaded_delta_dirs[0])

                    # check prod_delta_struct
                    print(f'Case 3: prod_delta_struct list: {self.prod_delta_struct.LOADED_FILES}')

                    delta_file = os.path.join(self.get_prod_delta_path(), unloaded_delta_dirs[0])
                    meta_data_xls_file = os.path.join(delta_file, 'Metadata', unloaded_delta_dirs[0] + '.xls')
                    # write to a pandas DataFrame
                    self.metadata_df = pd.read_excel(
                        xlrd.open_workbook(
                            filename=meta_data_xls_file,
                            encoding_override='cp1252'
                        )
                    )
                    # update the logfile
                    self.write_to_metadata_log(delimiter='\n', file_name=unloaded_delta_dirs[0])
                    print(f"Loaded file: {delta_files[0]} into a pandas DataFrame")
                    print(self.metadata_df.head())

        except Exception as e:
            print(e)





def load_new_metadata():
        S = StorageStruct()
        PD = MetaDataProd(is_historical=False)
        #PD.load_metadata()
        #md_log = PD.load_metadata_log()






def main():
  md_dev = MetaDataDev(is_historical=False)
  #md_dev.load_meta_data()
  md_dev.load_pickle()
  print(md_dev.previously_loaded_delta_data)


if __name__ == '__main__':
    main()