#!/usr/bin/env python2.7

"""
SystemUtils
===========
Module performs all system setup information.

"""

import os
import sys
import zipfile
import shutil
import subprocess
import glob
import cProfile
import threading
import numpy as np
from threading import Thread, Lock
from multiprocessing import Process, Pool, Lock
from datetime import datetime


class ConfigureFileDirectory:
    """
    ConfigureFileDirectory
    ----------------------
    Class performs all tasks related to file
    system processing. Specifically, the class
    collects all required files, loads them into a
    temporary directory and prepares the directory
    for securely transferring the files to the remote
    hadoop linux server.
    """

    def __init__(self, root_directory, file_extension):
        self.root_directory = root_directory    # type: str
        self.file_extension = file_extension    # type: str
        self.dest_directory = None              # type: str

        self.ext_files       = []               # type: list
        self.files_copied    = 0                # type: int
        self.file_copy_fail  = 0                # type: int
        self.total_file_size = 0.0              # type: float
        self.zipfile_name    = None             # type: str
        self.zipfile_created = False            # type: bool

    def create_directory(self, child_directory,  parent_directory):
        """
        Build a directory in specified location.
        :return:
        """
        try:
            if os.path.exists(parent_directory):
                if child_directory in os.listdir(parent_directory):
                    # - directory already exists
                    print("{} already exists in {}".format(child_directory, parent_directory))
                else:
                    # - create the new directory
                    os.mkdir(os.path.join(parent_directory, child_directory))
                self.dest_directory = os.path.join(parent_directory, child_directory)
        except OSError as e:
            print(e)
        finally:
            pass

    def load_target_files(self):
        """
        Load the files of the specific
        extension type into a list
        :return:
        """
        if os.path.exists(self.root_directory):
            os.chdir(self.root_directory)
            self.ext_files = glob.glob('*.'+self.file_extension)
            for file in self.ext_files:
                self.total_file_size += sys.getsizeof(file)
                try:
                    if file not in os.listdir(self.dest_directory):
                        f = os.path.join(os.getcwd(), file)
                        shutil.copy(f, self.dest_directory)
                        self.files_copied += 1
                except:
                    print("An error occurred when trying to copy: "+ file)
                    self.file_copy_fail += 1
            print("Total File Size[bytes]: {}".format(self.total_file_size))

    def create_archive(self):
        """
        Create an archived file in the
        specified location.
        :return:
        """
        # - status check to make sure the archive was properly built
        ZIP_SUCCESS = 0
        date_ = str(datetime.today())[:10].replace('-', '_')
        name_ = date_ + '_' + str(self.file_extension).upper() + '.zip'

        try:
            zipf = zipfile.ZipFile(name_, 'w', zipfile.ZIP_DEFLATED)
            self.zipfile_name = os.path.join(os.getcwd(), name_)
            for _, _, files in os.walk(self.dest_directory):
                for file in files:
                    try:
                        zipf.write(
                            # - full path to the file
                            os.path.join(self.root_directory, file),
                            # - write only the file to the archive
                            file
                        )
                    except:
                        print("Could not write file:"+file+" to zipfile")
        except:
            ZIP_SUCCESS = -1
            print("ZipFileError: Could not build archive.")
        finally:
            if ZIP_SUCCESS == 0:
                print("Archive Successfully Created")
                self.zipfile_created = True
            if ZIP_SUCCESS == -1:
                print("Failed to Build Archive")

    def delete_directory(self):
        """
        Delete the temporary directory
        once the zip file has been created.
        :return:
        """
        ZIP_DELETED = 0
        try:
            if os.path.exists(self.zipfile_name):
                shutil.rmtree(path=self.zipfile_name, ignore_errors=True)
            else:
                raise Exception("An error occurred while trying to delete archive")
        except Exception as e:
            ZIP_DELETED = -1
            print(e)
        finally:
            if ZIP_DELETED == 0:
                print("Successfully Deleted Archive")
            if ZIP_DELETED == -1:
                print("Failed to Delete Archive")

