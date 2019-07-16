"""
file_stats
~~~~~~~~~~
"""
import os
import csv
import sys
import numpy as np
import pandas as pd
from configparser import ConfigParser
from typing import Dict, Type, TypeVar, NewType, List
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

# admin log path
admin_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\admin_log'

def get_all_file_counts_by_Extension(file_path: str)  -> Dict[str, int]:
    """Get a dictionary that has the file extension as a key and the count of that file type."""
    try:
        if os.path.exists(file_path):
            # step 1: create a python Dict[str, int]
            file_type_counts = {}
            for i in os.listdir(file_path):
                current = os.path.splitext(i)[-1]
                # step 2: don't insert non file extension types fom the dict.
                if current == '':
                    pass
                else:
                    # step 3: if the type is not in the dict, add it, set the value to 1
                    if current not in list(file_type_counts.keys()):
                        file_type_counts[current] = 1
                    else:
                        # step 4: if the type has been seen, increment the counter
                        file_type_counts[current] += 1
            # step 5: create a data frame to write the data to
            df = pd.DataFrame(data=file_type_counts.values(), columns=['Count'], index=file_type_counts.keys())
            # step 6: fix the path name so it is writable
            path_name = os.path.split(file_path)[0].replace("\\", "_").replace(":","_")
            # step 7: write the data frame to a csv file
            df.to_csv(os.path.join(admin_log_path, f'all_file_info_{path_name}.csv'))
            return file_type_counts
    except Exception as e:
        print(e)


def get_file_count_by_extension(file_path: str, file_ext: str) -> int:
    """Get the count of files of a specified file extension from a given path"""
    try:
        if os.path.exists(file_path):
            file_ext_count = len(
                list(filter(lambda x: os.path.splitext(x)[-1] == '.'+file_ext,
                            list(map(lambda y: os.path.join(file_path, y),
                                     os.listdir(file_path)))))
            )
            return file_ext_count
    except (OSError, Exception) as e:
        logger.error(error=e)


def get_unique_file_types(file_path: str) -> List[str]:
    """Get a set consisting of the unique file types in the current directory"""
    supported_types = ['.docx', '.doc', '.eml', '.pdf', '.rtf']
    type_count = {}
    try:
        file_extensions = []
        for f in os.listdir(file_path):
            ext = os.path.splitext(f)[-1]
            if ext not in file_extensions and len(ext) > 0:
                type_count[ext] = 1
                file_extensions.append(ext)
        logger.info(info=f"Supported types: {supported_types}")
        logger.info(info=f"Unsupported types: {list(set(file_extensions) - set(supported_types))}")
        return file_extensions
    except (OSError, Exception) as e:
        logger.error(error=e)

def create_admin_spreadsheet(write_to_path: str, file_type: str, count: int,
                             count_extracted: int, count_failed: int,
                             failed_file_path: str, failed_file_name: list) -> None:
    """Creates an excel spreadsheet that contains all the relevant admin information"""
    try:
        with open(os.path.join(write_to_path, f'{file_type}_admin_log_{d}.csv'), mode='w', newline='\n') as f:
            fieldnames = ['file_type','count_total','count_extracted','count_failed','failed_file_path','failed_file_name']
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            # step 2: create the column names
            writer.writeheader()
            if len(failed_file_name) == 0 or failed_file_path is None:
                writer.writerow({'file_type': file_type, 'count_total': int(count), 'count_extracted': int(count_extracted),
                             'count_failed': int(count_failed), 'failed_file_path': 'None',
                             'failed_file_name': 'None'})
            else:
                writer.writerow(
                    {'file_type': file_type, 'count_total': int(count), 'count_extracted': int(count_extracted),
                     'count_failed': int(count_failed), 'failed_file_path': failed_file_path,
                     'failed_file_name': failed_file_name[0]})
                # step 3: iterate through the failed files list
                for i in failed_file_name[1:]:
                    writer.writerow({'file_type': '', 'count_total': '', 'count_extracted': '',
                              'count_failed': '', 'failed_file_path': failed_file_path,
                              'failed_file_name': failed_file_name[i]})
    except Exception as e:
        logger.error(error=e)



docx_files = get_file_count_by_extension(file_path=r'V:\Dev\Delta\20190619\Document', file_ext='docx')
get_all_file_counts_by_Extension(file_path=r'V:\Dev\Delta\20190619\Document')
# print(docx_files)
# all_file_types = get_unique_file_types(file_path=r'V:\Dev\Delta\20190619\Document')
# print(all_file_types)
#
# file_type_dict = get_all_file_counts_by_Extension(file_path=r'V:\Dev\Delta\20190619\Document')
# print(file_type_dict)
#
#
# get_unique_file_types(file_path= r'V:\Dev\Historical\20190521\Document')
#
#
# create_admin_spreadsheet(write_to_path=r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\admin_log',
#                          file_type='eml', count_extracted=eml_files, count_failed=1,
#                          failed_file_name='', failed_file_path='', count=eml_files)

#base_path = r'V:\Dev\Historical\20190521\Document'
#print(get_file_count_by_extension(file_path=base_path, file_ext='eml'))