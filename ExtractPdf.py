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
    """storage class to hold extracted pdfs data"""
    CURRENT_FILENAME = None
    FILE_COUNTER = 0
    PAGE = 0
    EXTRACTED_TEXT = None
    CONTAINER = dict()

    @classmethod
    def load_data(cls, data):
        """opens the current file
        a
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
        return cls.FILE_COUNTER.__int__()

    @classmethod
    def update_page(cls):
        cls.PAGE += 1
    @classmethod
    def get_page(cls):
        return cls.PAGE


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

    def extract_text(self, chunk_size=1):
        """extract text from the current pdf file"""

        # load in the file counter
        file_counter = self.data_container.get_file_counter()

        # loop through the chunked files
        while file_counter < chunk_size:
            # set the current file
            current_file = self.pdf_files[file_counter]
            # load in the current data set
            raw = self.data_container.load_data(data=current_file)
            # call PyPDF2 reader
            pdf_readr = PyPDF2.PdfFileReader(raw)

            # call page counter
            page_counter = self.data_container.PAGE

            # temporary list to hold each page of cleaned text
            temp_list = [] # type: List[String]

            while page_counter < pdf_readr.getNumPages():

                page = pdf_readr.getPage(page_counter)

                # create a list of extracted text
                pdf_to_text = list(str(page.extractText()).splitlines())
                pdf_str = ''.join(pdf_to_text)

                # process text before loading into pandas DataFrame
                cleaned_pdf_str = re.sub('(\s+|\t|\r|\n)', ' ', page.extractText()) \
                                                                    .strip() \
                                                                    .lower()

                name = current_file + ' page {}'.format(page_counter)
                # load in the text
                temp_list.append(cleaned_pdf_str)

                page_counter += 1
                pprint("page counter: {}".format(page_counter))

            # load in the full pdf
            self.data_container.CONTAINER.update({self.pdf_files[file_counter]:
                                                        ''.join(temp_list)})

            # increment the file counter
            self.data_container.update_file_counter()
            file_counter = self.data_container.get_file_counter()
            print("num files read: {}".format(file_counter))

            pprint(self.data_container.CONTAINER)
            # close the opened pdf
            raw.close()


def main():

    filepath = "FILEPATH"
    extract_pdf = ParsePdf(f_path=filepath)
    extract_pdf.extract_text()





if __name__ == '__main__':
    main()






