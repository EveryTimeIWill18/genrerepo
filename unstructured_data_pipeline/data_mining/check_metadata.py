"""
check_metadata
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


class Outer:
    """Outer Class"""
    def __init__(self):
        self.inner = self.Inner()

    def reveal(self):
        """Call the Inner class function display"""
        self.inner.inner_display("Calling Inner class function from Outer class.")

    class Inner:
        """Inner Class"""

        def inner_display(self, msg):
            """Inner class method"""
            print(f'Class: {self.__class__.__name__}\nMessage: {msg}')

    class _Inner:
        """Second Inner Class"""

        def inner_display(self, msg):
            """Inner class method"""
            print(f'Class: {self.__class__.__name__}\nMessage: {msg}')





def memorize(f: Callable):
    """
    Stores the meta data files that have already been
    loaded into the
    :param f:
    :return:
    """
    # lists for each directory
    dev_delta  = []
    dev_hist   = []
    prod_delta = []
    prod_hist  = []

    # counters for each list
    dev_delta_cntr  = 0
    dev_hist_cntr   = 0
    prod_delta_cntr = 0
    prod_hist_cntr  = 0

    @wraps(f)
    def wrapper(is_prod: bool, is_historical: bool):
        nonlocal dev_delta, dev_hist, prod_delta, prod_hist
        nonlocal dev_delta_cntr, dev_hist_cntr
        nonlocal prod_delta_cntr, prod_hist_cntr

        # load the function
        func = f(is_prod, is_historical)

        # case 1: prod
        if is_prod:
            if is_historical:
                try:
                    if func[prod_hist_cntr] not in prod_hist:
                        current = func[prod_hist_cntr]
                        prod_hist.append(func[prod_hist_cntr])
                        prod_hist_cntr += 1
                        print('Prod > Historical')
                        print('------------')
                        print(f'prod_hist = {prod_hist}')
                        print(f'prod_delta = {prod_delta}')
                        print(f'dev_hist = {dev_hist}')
                        print(f'dev_delta = {dev_delta}')
                        print('------------')
                        print('\n')
                        # get the meta data path
                        prod_hist_metadata = os.path.join(
                            PROD_HIST, current, 'Metadata', current + '.xls'
                        )
                        # get the document path
                        prod_hist_document = os.path.join(
                            PROD_HIST, current, 'Document'
                        )
                        if os.path.isfile(prod_hist_metadata):
                            # load the metadata file into a pandas DataFrame
                            metadata_df = pd.read_excel(
                                xlrd.open_workbook(
                                    filename=prod_hist_metadata,
                                    encoding_override='cp1252'
                                )
                            )
                            # update the text file
                            with open(HIST_PROD_LOG, 'a+') as file_:
                                current = current + '\n'
                                file_.write(current)
                        return prod_hist_document, metadata_df, current
                except IndexError:
                    print('No files left in Prod > Historical')
            else:
                try:
                    if func[prod_delta_cntr] not in prod_delta:
                        current = func[prod_delta_cntr]
                        prod_delta.append(func[prod_delta_cntr])
                        prod_delta_cntr += 1
                        print('Prod > Delta')
                        print('------------')
                        print(f'prod_hist = {prod_hist}')
                        print(f'prod_delta = {prod_delta}')
                        print(f'dev_hist = {dev_hist}')
                        print(f'dev_delta = {dev_delta}')
                        print('------------')
                        print('\n')
                        # get the meta data path
                        prod_delta_metadata = os.path.join(
                            PROD_DELTA, current, 'Metadata', current + '.xls'
                        )
                        # get the document path
                        prod_delta_document = os.path.join(
                            PROD_DELTA, current, 'Document'
                        )
                        if os.path.isfile(prod_delta_metadata):
                            # load the metadata file into a pandas DataFrame
                            metadata_df = pd.read_excel(
                                xlrd.open_workbook(
                                    filename=prod_delta_metadata,
                                    encoding_override='cp1252'
                                )
                            )
                            # update the text file
                            with open(DELTA_PROD_LOG, 'a+') as file_:
                                current = current + '\n'
                                file_.write(current)

                        return prod_delta_document, metadata_df, current
                except IndexError:
                    print('No files left in Prod > Delta')
        # case 2: delta
        else:
            if is_historical:
                try:
                    if func[dev_hist_cntr] not in dev_hist:
                        current = func[dev_hist_cntr]
                        dev_hist.append(func[dev_hist_cntr])
                        dev_hist_cntr += 1
                        print('Dev > Historical')
                        print('------------')
                        print(f'prod_hist = {prod_hist}')
                        print(f'prod_delta = {prod_delta}')
                        print(f'dev_hist = {dev_hist}')
                        print(f'dev_delta = {dev_delta}')
                        print('------------')
                        print('\n')
                        # get the meta data path
                        dev_hist_metadata = os.path.join(
                            DEV_HIST, current, 'Metadata', current + '.xls'
                        )
                        # get the document path
                        dev_hist_document = os.path.join(
                            DEV_HIST, current, 'Document'
                        )
                        if os.path.isfile(dev_hist_metadata):
                            # load the metadata file into a pandas DataFrame
                            metadata_df = pd.read_excel(
                                xlrd.open_workbook(
                                    filename=dev_hist_metadata,
                                    encoding_override='cp1252'
                                )
                            )
                        # update the text file
                        with open(HIST_DEV_LOG, 'a+') as file_:
                            current = current + '\n'
                            file_.write(current)
                        return dev_hist_document, metadata_df, current
                except IndexError:
                    print('No files left in Dev > Historical')
            else:
                try:
                    if func[dev_delta_cntr] not in dev_delta:
                        current = func[dev_delta_cntr]
                        dev_delta.append(func[dev_delta_cntr])
                        dev_delta_cntr += 1
                        print('Dev > Delta')
                        print('------------')
                        print(f'prod_hist = {prod_hist}')
                        print(f'prod_delta = {prod_delta}')
                        print(f'dev_hist = {dev_hist}')
                        print(f'dev_delta = {dev_delta}')
                        print('------------')
                        print('\n')
                        # get the meta data path
                        dev_delta_metadata = os.path.join(
                            DEV_DELTA, current, 'Metadata', current + '.xls'
                        )
                        # get the document path
                        dev_delta_document = os.path.join(
                            DEV_DELTA, current, 'Document'
                        )
                        if os.path.isfile(dev_delta_metadata):
                            # load the metadata file into a pandas DataFrame
                            metadata_df = pd.read_excel(
                                xlrd.open_workbook(
                                    filename=dev_delta_metadata,
                                    encoding_override='cp1252'
                                )
                            )
                        # update the text file
                        with open(DELTA_DEV_LOG, 'a+') as file_:
                            current = current + '\n'
                            file_.write(current)
                        return dev_delta_document, metadata_df, current
                except IndexError:
                    print('No files left in Dev > Delta')
    # return the wrapped function
    return wrapper


@memorize
def load_meta_data(is_prod: bool, is_historical: bool):
    """load in the current meta data file"""

    if is_prod:
        if is_historical:
            hist = os.listdir(PROD_HIST)
            return hist
        else:
            delta = os.listdir(PROD_DELTA)
            return delta
    else:
        if is_historical:
            hist = os.listdir(DEV_HIST)
            return hist
        else:
            delta = os.listdir(DEV_DELTA)
            return delta


def load_metadata(is_prod: bool, is_historical: bool):
    """Load in the metadata"""
    try:
        pass
    except (Exception, IndexError) as e:
        print(e)

def get_metadata_log(full_file_name: str):
    """Load in the current metadata log"""
    loaded_metadata = []
    with open(full_file_name, 'r') as input_file:
        for line in input_file.readlines():
            loaded_metadata.append(line.rstrip('\n'))
    return loaded_metadata



#print(get_metadata_log(full_file_name=HIST_DEV_LOG))

#dir_, df, current = load_meta_data(is_prod=True, is_historical=False)

# df2 = load_meta_data(is_prod=True, is_historical=False)
# df3 = load_meta_data(is_prod=True, is_historical=False)
# df4 = load_meta_data(is_prod=True, is_historical=False)
# df5 = load_meta_data(is_prod=False, is_historical=False)