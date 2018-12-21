# -*- coding: utf-8 -*-
import socket
import socks
import sys
import os
import glob
import paramiko
from multiprocessing import (Pool, Process,
                             ProcessError, Lock, Queue)
import unicodedata
import numpy as np
import pickle
import threading
from Queue import Queue
from functools import partial
from pprint import pprint
import sshtunnel
from datetime import datetime

# winscp script to load files
# scp //ustrzb37c1/cifs_air_01/WinRisk/*Analytics/AutoBi_2k_2016/converted_html/andrews.pdf.html ggregor@mseaulxda03:/genre/bda/apps/claimsops/autobi_2k_2016.scp/

# global variables
# -------------------
LOADED_FILES = list()   # global list containing previously loaded eml files
ERROR_FILES = list()    # list of files where their format raises an error
FILE_COUNTER = int(0)   # loaded file counter

def create_sftp_connection(host, port, username, password,
                           local_path, remote_path,
                           chunksize=None, single_file=False):
    """make connection to server"""
    # list of loaded files imported in
    global LOADED_FILES, FILE_COUNTER, ERROR_FILES

    sock = socks.socksocket()
    counter = FILE_COUNTER

    # set proxy connection values
    sock.set_proxy(
        proxy_type=None,
        addr=host,
        port=port,
        username=str(username),
        password=str(password)
        )
    # connect the socket
    sock.connect((host, port))
    try:
        if socket.gethostname() is not None:
            print("Host: {}".format(socket.gethostname()))
            print(socket.getfqdn())
            print("-------------")
            # create transport
            sftp = paramiko.Transport(sock)
            sftp.connect(
                username=str(username),
                password=str(password)
            )
            if sftp.is_alive():
                print("sftp connection is alive")
                # create client
                client = paramiko.SFTPClient.from_transport(sftp)

                # load in the files to be transferred
                fn = 'pdf_df{}'.format(str(datetime.today())[:10])
                os.chdir(local_path)
                files_ = glob.glob("*.zip")
                print  "files_ : {}".format(files_)

                #remote_files = client.listdir(str(remote_path))
                #print("Remote Files\n---------")
                #pprint(remote_files)

                if chunksize is None:
                    while counter < files_.__len__():
                        if files_[counter] not in LOADED_FILES:
                            print("Loading file: {}".format(files_[counter]))
                            client.put(localpath=files_[counter],
                                       remotepath=str(remote_path) + "/" + str(files_[counter]))
                            LOADED_FILES.append(files_[counter])
                            counter += 1
                            FILE_COUNTER = counter
                        else:
                            print("file previously loaded")
                else:
                    while counter < int(chunksize):
                        print("Loading file: {}".format(files_[counter]))
                        client.put(localpath=files_[counter],
                                   remotepath=str(remote_path) + "/" + str(files_[counter]))
                        LOADED_FILES.append(files_[counter])
                        counter += 1
                        FILE_COUNTER = counter
            else:
                print("Could not successfully connect")
        else:
            raise socket.SO_ERROR("[Errno 2] SO_ERROR: Socket error occured")
    except socket.SO_ERROR as e:
        print(str(e))
    finally:
        client.close()  # close client application
        sftp.close()    # clse fstp application
        if sftp.is_alive() is False:
            print("Successfully disconnected")
            #files_to_load(file_path=str(pickle_path))

def files_to_load(file_path, file_type):
    """Load files"""
    try:
        if os.path.exists(file_path):
            os.chdir(file_path)
            # change back after
            files_ = glob.glob("*.{}".format(file_type)) # os.listdir(os.getcwd())
            return files_
        else:
            raise  OSError("[Errno 3] OSError: Could not find path\n")
    except OSError as e:
        print(str(e))

def create_pickle_file(file_path, file_name):
    """file that stores the list of
    already loaded files into linux"""
    global LOADED_FILES
    EXIT_SUCCESS = 1
    try:
        if os.path.exists(str(file_path)):
            try:
                save_ = str(file_path) + '\\' + str(file_name) + ".pickle"
                output = open(save_, 'wb')
                pickle.dump(LOADED_FILES, output)
            except pickle.PicklingError:
                EXIT_SUCCESS = -1  # failure creating pickle file
                print("[PicklingError]: Could not pickle obj")
        else:
            raise OSError("[Pickling error] OSError: Could not find path to pickle to.")
    except OSError as e:
        print(str(e))
    finally:
        output.close()

def load_sftp_pickle_files(pickle_path, pickle_file):
    """load previously loaded pickle files"""
    global LOADED_FILES
    EXIT_SUCCESS = 1
    try:
        if os.path.exists(pickle_path):
            os.chdir(pickle_path)
            try:
                pkl_file_ = "{}.pickle".format(pickle_file)
                # open pickle file
                f = open(pkl_file_, "rb")
                LOADED_FILES = list(pickle.load(f))
            except pickle.PicklingError:
                EXIT_SUCCESS = -1  # failure creating pickle file
                print("[PicklingError]: Could not load pickle obj")
        else:
            raise OSError("[Errno 3] OSError: Could not find path\n")
    except OSError as e:
        print(str(e))
    finally:
        f.close()
