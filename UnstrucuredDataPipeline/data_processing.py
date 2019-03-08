"""
process_data
~~~~~~~~~~~~
This module is used to efficiently pre-process the unstructured data
to be prepared for loading into the hadoop ecosystem.

"""
import os
import io
import sys
import csv
import time
import mmap
import shutil
import threading
import multiprocessing
import numpy as np
from collections import OrderedDict
from collections import  Counter
from collections import  ChainMap
from pathlib import Path
from pprint import pprint
from data_processing_namespace import *


class FileSystemsSetup(object):
    """
    FileSystemsSetup
    ~~~~~~~~~~~~~~~~
    This class is used to efficiently
    copy over files to maintain the original
    files information.
    """

    def __init__(self):
        self._files: dict = {}
        self._file_statistics: dict = {}
        self._file_chunks: list = []
        self._thread_lock = threading.Lock()
        #self._process_pool = multiprocessing.Pool(processes=5)

    def file_stats(self, directory: str, file_ext: str):
        """
        Get the current file system's makeup.
        :return:
        """
        counter = 0
        try:
            if file_ext not in self._file_statistics:
                with os.scandir(directory) as it:
                    for entry in it:
                        if os.path.splitext(entry)[-1] == '.'+file_ext:
                            counter += 1
                            if entry not in self._files:
                                self._files[counter] = {os.path.join(directory, entry): os.path.basename(entry)}
                self._file_statistics[file_ext] = counter
        except OSError:
            print("An error has occurred while calculating the number of files.")

    def copy_files(self, write_directory: str, directory:str, file_ext: str):
        """
        Copy over files for processing.
        This will be run on a single process
        :return:
        """
        try:
            # - check to see if directory for the current extension exists
            if not os.path.exists(os.path.join(write_directory, file_ext)):
                os.mkdir(os.path.join(write_directory, file_ext))  # create if it does not exist
            time.sleep(3)   # wait a few moments until directory is made

            chunk_size = np.floor(len(self._file_statistics[file_ext])/4)
            t1 = threading.Thread(target=self.process_file_chunk, args=(directory, write_directory, 0, ))
            t2 = threading.Thread(target=)
            t3 = threading.Thread()
            t4 = threading.Thread()

        except:
            pass
        finally:
            pass

    def set_file_chunks(self, file_ext: str, position=0):
        """
        Recursively break up the list of files
        into a list of sub-lists.
        :param position:
        :return:
        """
        if position >= self._file_statistics[file_ext]:
            self._file_chunks.pop(-1)
            self._file_chunks.append(self._file_statistics[file_ext])
            print("finished creating sub-lists: length of files: {}".format(self._file_statistics[file_ext]))
        else:
            self._file_chunks.append(int(position))
            return self.set_file_chunks(file_ext,
                    position=position+np.floor(self._file_statistics[file_ext]/4))


    def process_file_chunk(self, directory: str, write_directory: str, begin: int, end: int):
        """
        Process a portion of the files
        in the selected directory.
        :return:
        """
        # - change current directory
        os.chdir(write_directory)
        try:
            while begin < end:
                try:
                    index = list(self._files[begin].keys())[0]
                    current_file = self._files[begin].keys(index)
                    if current_file not in os.getcwd():
                        shutil.copy2(
                            os.path.join(directory, current_file),
                            os.path.join(write_directory, current_file)
                        )
                        begin += 1 # increment counter
                except:
                    pass

        except OSError:
            pass
        finally:
            pass

    def run(self):
        """
        Run the file processing.
        :return:
        """
        pass

#f = len(list(Path(personalUmbrella).glob('*.docx')))

file_system = FileSystemsSetup()
file_system.file_stats(directory=personalUmbrella, file_ext='docx')
index = list(file_system._files[1].keys())[0]

file_system.set_file_chunks(file_ext='docx')
pprint(file_system._file_chunks)
