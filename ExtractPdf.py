import os
import io
import re
import glob
import shutil
import pickle
import functools
import PyPDF2
import numpy as np
import pandas as pd
import types
from typing import (List, Set, Dict, Tuple, Text,
                    Optional, Iterable, Callable, Union, Counter)
from pprint import pprint
from datetime import datetime

class FileCounter: # type: [Callable]
    """Class decorator for creating a class counter"""
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
        self.counter = 0

    def __str__(self):
        """:returns current value of self.counter"""
        return self.counter

    def __call__(self, *args, **kwargs):
        """:returns callable and increments the counter"""
        self.counter += 1
        return self.func(*args, **kwargs)


class DataContainer: #type: CONTAINER[Dict]
    """storage class to hold extracted pdf data"""
    CURRENT_FILENAME = None   #type: String
    FILE_COUNTER     = 0      #type: Integer
    PAGE             = 0      #type: Integer
    EXTRACTED_TEXT   = None   #type: String
    CONTAINER        = dict() #type: Dict[String]
    DATAFRAME        = None   #type: DataFrame[String]

    @classmethod
    def load_data(cls, data):
        """opens the current file
        :returns raw_data[type: file]
        """
        raw_data = None
        try:
            raw_data = open(data, 'rb')
            return raw_data
        except:
            print("Error opening pdf file")
            print("Closing file")
            raw_data.close()
        finally:
            print("Successfully opened: {}".format(data))

    @classmethod
    @FileCounter
    def update_file_counter(cls):
        """increment file counter"""
        cls.FILE_COUNTER = cls.update_file_counter.counter

    @classmethod
    @FileCounter
    def get_file_counter(cls):
        """:returns integer file counter"""
        return cls.FILE_COUNTER.__int__()

    @classmethod
    def update_page(cls):
        cls.PAGE += 1
    @classmethod
    def get_page(cls):
        return cls.PAGE
    @classmethod
    def reset_page(cls):
        cls.PAGE = 0

    @classmethod
    def to_DataFrame(cls):
        """convert the dictionary
        to a pandas DataFrame
        """
        EXIT_SUCCESS = 0;
        try:
            if cls.CONTAINER is not None:
                cls.DATAFRAME = pd.DataFrame(cls.CONTAINER)
            else:
                raise TypeError("Error: cls.CONTAINER is not of type: Dict[String]")
        except TypeError as e:
            EXIT_SUCCESS = -1   # type error has occurred
            print(e)
        finally:
            if EXIT_SUCCESS == 0:
                print("DataFrame successfully created")
            else:
                print("Could not create DataFrame")


class ParsePdf(object):
    """Extract data from pdfs"""

    def __init__(self, f_path):
        # if directory could not be found
        self.pdf_files = None
        try:
            # assert that the directory exists
            if os.path.exists(f_path):
                self.f_path = f_path
                os.chdir(self.f_path)
                print("current directory: {}".format(os.getcwd()))
                # get a list of all pdf files in current directory
                self.pdf_files = glob.glob('*.pdf')
            else:
                raise OSError("Error: path not found")
        except OSError as e:
            print(e)

        # load in container class
        self.data_container = DataContainer()

        # list containing the files that have already been loaded
        self.loaded_files = []

        # error files
        self.error_files = []

    def extract_text(self, chunk_size=1):
        """extract text from the current pdf file"""

        # loop through the chunked files
        while self.data_container.get_file_counter() < chunk_size:
            # set the current file
            self.data_container.CURRENT_FILENAME = self.pdf_files[self.data_container.get_file_counter()]
            current_file = self.pdf_files[self.data_container.get_file_counter()]
            # load in the current data set
            raw = self.data_container.load_data(data=current_file)
            # call PyPDF2 reader
            pdf_readr = PyPDF2.PdfFileReader(raw)

            # temporary list to hold each page of cleaned text
            temp_list = [] # type: List[String]

            while  self.data_container.get_page() < pdf_readr.getNumPages():

                page = pdf_readr.getPage(self.data_container.get_page())

                # create a list of extracted text
                try:
                    pdf_to_text = list(str(page.extractText()).splitlines())
                    pdf_str = ''.join(pdf_to_text)

                    # process text before loading into pandas DataFrame
                    cleaned_pdf_str = re.sub('(\s+|\t|\r|\n)', ' ', page.extractText()) \
                        .strip() \
                        .lower()

                    name = current_file + ' page {}'.format(self.data_container.get_file_counter())
                    # load in the text
                    temp_list.append(cleaned_pdf_str)

                    # update the page counter
                    self.data_container.update_page()
                    pprint("page counter: {}".format(self.data_container.get_page()))
                except:
                    # store the files that raise an error
                    print("\nAN ERROR HAS OCCURRED!")
                    print("\n\t-------------\n\tEncoding Error Raised: File: {}."
                          " Could not extract text\n\t--------------\n\t".format(current_file))
                    self.error_files.append(current_file)
                    break

            # load in the full pdf
            self.data_container.CONTAINER.update({self.pdf_files[self.data_container.get_file_counter()]:
                                                        ''.join(temp_list)})
            # reset the page counter
            self.data_container.reset_page()

            # increment the file counter
            self.data_container.update_file_counter()
            file_counter = self.data_container.get_file_counter()
            print("num files read: {}".format(file_counter))

            #pprint(self.data_container.CONTAINER)
            # close the opened pdf
            raw.close()

    def to_pickle_file(self, pickle_path):
        """save the data as a pickle file"""
        fn = 'doc_to_pdf{}'.format(str(datetime.today())[:10])
        EXIT_STATUS = 0
        try:
            if os.path.exists(pickle_path):
                os.chdir(pickle_path)
                try:
                    save_ = str(pickle_path) + '\\' + fn + '.pickle'
                    pkl_out = open(save_, 'wb')
                    pickle.dump(self.data_container.to_DataFrame())
                except pickle.PicklingError:
                    EXIT_STATUS = -1
                    print("[PicklingError]: Could not pickle obj")
                    print("EXIT_SUCCESS: {}".format(EXIT_STATUS))
            else:
                raise OSError("Error: path not found")
        except OSError as e:
            print(e)
        finally:
            print("finishing to_pickle_file with exit status: {}".format(EXIT_STATUS))
            pkl_out.close() # close pkl_out



def main():

    filepath = ""
    extract_pdf = ParsePdf(f_path=filepath)
    extract_pdf.extract_text(chunk_size=10)

    raw_data = extract_pdf.data_container.CONTAINER
    s1 = pd.Series(list(raw_data.keys()), index=list(raw_data.keys()))
    s2 = pd.Series(raw_data.values(), index=list(raw_data.keys()))

    df = s1.to_frame(name='File_Names').join(s2.to_frame(name='Text_Data'))
    pprint(df)






if __name__ == '__main__':
    main()












