"""
metadata_config
~~~~~~~~~~~~~~
"""
import os
import csv
import stat
import xlrd
import time
import pickle
import pandas as pd
from functools import wraps, partial, reduce
from datetime import datetime, timedelta
from typing import Dict, List, Sequence, Set, Text, Tuple, Callable

# meta data logging information
METADATA_LOG = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
DELTA_LOG = os.path.join(METADATA_LOG, 'delta')
HIST_LOG = os.path.join(METADATA_LOG, 'historical')

# meta data log files
DELTA_DEV_LOG = os.path.join(DELTA_LOG, 'dev_metadata_log.txt')
DELTA_PROD_LOG = os.path.join(DELTA_LOG, 'prod_metadata_log.txt')
HIST_DEV_LOG = os.path.join(HIST_LOG, 'dev_metadata_log.txt')
HIST_PROD_LOG = os.path.join(HIST_LOG, 'prod_metadata_log.txt')

# meta data file paths
PROD = r'V:\Prod'
DEV = r'V:\Dev'
PROD_DELTA = os.path.join(PROD, 'delta'.title())
PROD_HIST = os.path.join(PROD, 'historical'.title())
DEV_DELTA =  os.path.join(DEV, 'delta'.title())
DEV_HIST = os.path.join(DEV, 'historical'.title())



def get_metadata_log(full_file_name: str):
    """Load in the current metadata log file."""
    loaded_metadata = []
    with open(full_file_name, 'r') as input_file:
        for line in input_file.readlines():
            loaded_metadata.append(line.rstrip('\n'))
    return loaded_metadata


def append_to_metadata_log(full_file_name: str, data: list):
    """Append to the current metadata log file."""
    with open(full_file_name, 'w') as output_file:
        for i in data:
            value = i + '\n'
            output_file.write(value)


def get_queued_files(metadata_log: str, metadata_path: str):
    """Get the metadata files that need to be loaded.
    metadata_path: Should be one of:
        PROD_DELTA
        PROD_HIST
        DEV_DELTA
        DEV_HIST

    metadata_log: Should be one of:
        DELTA_DEV_LOG
        DELTA_PROD_LOG
        HIST_DEV_LOG
        HIST_PROD_LOG
    """
    metadata_files = os.listdir(metadata_path)
    loaded_metadata = get_metadata_log(metadata_log)
    unloaded_files = list(set(metadata_files) - set(loaded_metadata))
    return loaded_metadata, unloaded_files

def load_document_path(metadata_path: str, queued_dir: str):
    """Load the document path for the corresponding metadata."""
    try:
        queued_document_dir = os.path.join(metadata_path, queued_dir, 'Document')
        return queued_document_dir
    except Exception as e:
        print(e)

def load_metadata_file(metadata_log: str, metadata_path: str) -> Tuple[str, dict]:
    """Load the current metadata file into a pandas DataFrame.

      :arg metadata_log - the full file path to the metadata.txt file:
      :arg metadata_path - the path to the dms data:

      :returns tuple[str, dict]:
    """
    try:
        # step 1: get the list of unloaded dirs
        loaded, queued = get_queued_files(metadata_log=metadata_log, metadata_path=metadata_path)

        # step 2: get the full path to the unloaded documents
        queued_doc_dir = load_document_path(metadata_path=metadata_path, queued_dir=queued[0])
        print(f'loaded files := {loaded}')
        print(f'queued files := {queued}')
        print(f'queued_doc_dir := {queued_doc_dir}')
        print(f'Number of files[queued_doc_dir] := {len(os.listdir(queued_doc_dir))}')

        metadata_file = os.path.join(metadata_path, queued[0], 'Metadata', queued[0]+'.xls')
        print(f'metadata_file := {metadata_file}')
        metadata_df = pd.read_excel(
            xlrd.open_workbook(
                filename=metadata_file,
                encoding_override='cp1252'
            )
        )

        loaded_files = [*loaded, queued[0]]
        print(f'loaded files := {loaded_files}')
        append_to_metadata_log(full_file_name=metadata_log, data=loaded_files)

        return queued_doc_dir, metadata_df.to_dict()

    except IndexError:
        print(f'All files in {metadata_path} have been loaded successfully.')

    except Exception as e:
        print(e)

#load_metadata_file(metadata_log=DELTA_PROD_LOG, metadata_path=PROD_DELTA)


#hist_dev_log = get_metadata_log(full_file_name=DELTA_PROD_LOG)
#files_to_load = get_queued_files(metadata_log=DELTA_PROD_LOG, metadata_path=PROD_DELTA)
