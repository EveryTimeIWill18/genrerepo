"""
meta_data
~~~~~~~~~
Load in the meta data file.
NOTE:
    Important Columns:
        - Object_id
        - Object_name
        - Prior_Version_Object_Name
        - Version
        - File_name
        - Format
        - claim_id
"""
import os
import csv
import xlrd
import pandas as pd
from pprint import pprint
from datetime import datetime, timedelta
from unstructured_data_pipeline.data_mining.data_parsers import d

pd.set_option("display.max_rows", 101)
pd.set_option("display.max_columns", 50)

# base path for the metadata log
meta_data_log_dir = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
delta_log = os.path.join(meta_data_log_dir, 'delta')
historical_log = os.path.join(meta_data_log_dir, 'historical')

# test meta data excel file
meta_data_xls =  r'V:\Dev\Delta\20190619\Metadata\20190619.xls'



class MetaData(object):
    """Load in the metadata file"""

    def __init__(self, metadata_log_file: str):
        self._metadata_log_file: str = metadata_log_file
        self.last_loaded_metadata_file: str = None
        self.base_data_path: str = None
        self.prod_dev_path: str = None
        self.delta_hist_path: str = None
        self.full_data_path: str = None
        self.loaded_metadata: list = [] # stores the files that have already been loaded into hive


    def write_to_metadata_log(self, delimiter: str, file_name: str):
        """update the metadata log file"""
        try:
            if os.path.isfile(file_name):
                # step 1: open the metadata log file using a context manager
                with open(self._metadata_log_file, 'w', newline='\n') as f:
                    # step 1.1: write to the log file, the current meta data log file that's been loaded
                    metadata_writer = csv.writer(
                        f, delimiter=delimiter,
                        quoting=csv.QUOTE_MINIMAL
                    )
                    # step 1.2: write the filename to the metadata log file
                    metadata_writer.writerow(os.path.splitext(os.path.basename(file_name))[0])
            else:
                print('file not found')
        except Exception as e:
            print(e)

    def read_metadata_log(self, metadata_log_path: str, metadata_logfile: str):
        """read in the metadata log file to get the last run metadata file"""
        with open(os.path.join(metadata_log_path, metadata_logfile)) as f:
            if len(f.read()) == 0:
                print('No file read yet')
            for row in f.readlines():
                self.loaded_metadata.append(row)
                # self.last_loaded_metadata_file = row
                # print(f'Last loaded metadata file: {self.last_loaded_metadata_file}')


    def load_metadata(self, file_path: str, is_historical=True, is_prod=False):
        """"""
        data_set: str   = None
        meta_data: str  = None
        try:
            # step 1: check if prod or dev
            if is_prod:
                self.prod_dev_path = r'V:\Prod'
                # step 1.1: check if historical or delta
                if is_historical:
                    self.delta_hist_path = r'Historical'
                    # step 2: load in the prod_metadata_log file
                    self.read_metadata_log(
                        metadata_log_path=historical_log,
                        metadata_logfile=r'prod_metadata_log.txt'
                    )
                    print(f"Currently loaded meta data files: {self.loaded_metadata}")
                    # step 3: if the list is empty, no data has bee loaded
                    meta_data_files = os.listdir(os.path.join(self.prod_dev_path, self.delta_hist_path))
                    if len(self.loaded_metadata) == 0:
                        pass

                else:
                    self.delta_hist_path = r'Delta'
            else:
                self.prod_dev_path = r'V:\Dev'
                if is_historical:
                    self.delta_hist_path = r'Historical'
                else:
                    self.delta_hist_path = r'Delta'
        except Exception as e:
            print(e)


    def load_metadata_file(self, full_file_path: str):
        """
        load the current metadata file

        :param full_file_path:
        :return:
        """
        try:
            if os.path.isfile(full_file_path) and os.path.splitext(full_file_path)[-1] == '.xls':
                metadata_file = pd.read_excel(
                    xlrd.open_workbook(filename=full_file_path, encoding_override="cp1252")
                )
                # step 2: extract only the necessary columns from the meta_data data frame
                md_df = metadata_file.loc[:, ['claim_id', 'Object_id', 'File_Name', 'Format',
                                             'Version', 'Object_name', 'Prior_Version_Object_Name']]
                print(md_df.head())
                # step 2.1: convert the meta data to a dictionary so it can be successfully serialized into python 2
                # NOTE: must be converted to avoid pandas DataFrame version error('no module named managers')
                # step 3: update the metadata log csv file
                self.write_to_metadata_log(delimiter='\n', file_name=full_file_path)
                return md_df.to_dict()
            else:
                raise OSError(f'OSError: File: {os.path.basename(full_file_path)} not found.')
        except OSError as e:
            print(e)

    def load_metadata_as_df(self, full_file_path: str):
        """
        load the current metadata file

        :param full_file_path:
        :return:
        """
        try:
            if os.path.isfile(full_file_path) and os.path.splitext(full_file_path)[-1] == '.xls':
                metadata_file = pd.read_excel(
                    xlrd.open_workbook(filename=full_file_path, encoding_override="cp1252")
                )
                # step 2: extract only the necessary columns from the meta_data data frame
                md_df = metadata_file.loc[:, ['claim_id', 'Object_id', 'File_Name', 'Format',
                                             'Version', 'Object_name', 'Prior_Version_Object_Name']]
                print(md_df.head())
                # step 2.1: convert the meta data to a dictionary so it can be successfully serialized into python 2
                # NOTE: must be converted to avoid pandas DataFrame version error('no module named managers')
                # step 3: update the metadata log csv file
                self.write_to_metadata_log(delimiter='\n', file_name=full_file_path)
                return md_df
            else:
                raise OSError(f'OSError: File: {os.path.basename(full_file_path)} not found.')
        except OSError as e:
            print(e)





def main():
    meta_data = MetaData(metadata_log_file=os.path.join(meta_data_log_dir, 'metadata_log.txt'))
    md_dict = meta_data.load_metadata_file(full_file_path=meta_data_xls)
    #md_df = pd.DataFrame.from_dict(md_dict)
    #pprint(md_df.head())
    #meta_data.read_metadata_log()
    #pprint(f'metadata dictionary: {md_dict}')

if __name__ == '__main__':
    main()










#
# def main():
#     md_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
#     md_file = 'metadata_log_06_14_2019'
#
#     metadata_test_file_path = r'V:\Dev\Historical\20190521\Metadata'
#     metadata_test_file = r'20190521.xls'
#
#     metadata = MetaData(metadata_log_file=os.path.join(md_log_path, md_file))
#     meta_data_df = metadata.load_metadata_file(full_file_path=os.path.join(metadata_test_file_path, metadata_test_file))
#     md_df = meta_data_df.loc[:, ['claim_id', 'Object_id', 'File_Name', 'Format',
#                               'Version', 'Object_name', 'Prior_Version_Object_Name',
#                               ]]
#     print(md_df.head())
# if __name__ == '__main__':
#     main()