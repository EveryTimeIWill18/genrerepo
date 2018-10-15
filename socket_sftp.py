import socket
import socks
import sys
import os
import glob
import paramiko
from multiprocessing import (Pool, Process,
                             ProcessError, Lock, Queue)
import numpy as np
import pickle

# --- Global Variables

# uploaded files list
LOADED_FILES = list()

# number of upoaded files
FILE_COUNTER = int(0)

def create_sftp_connection(host, port, username, password, local_path, remote_path, chunksize=None):
    """make connection to server"""

    sock = socks.socksocket()
    try:
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
                fstp = paramiko.Transport(sock)
                fstp.connect(
                    username=str(username),
                    password=str(password)
                )
                if fstp.is_alive():
                    print("\nfstp connection is alive")
                    # create client
                    client = paramiko.SFTPClient.from_transport(fstp)

                    # list of loaded files imported in
                    global LOADED_FILES, FILE_COUNTER

                    # load in the files to be transferred
                    files_ = files_to_load(local_path)
                    counter = 0
                    if chunksize is None:
                        while counter < files_.__len__():
                            client.put(localpath=files_[counter],
                                       remotepath=str(remote_path) + "/" + str(files_[counter]))
                            LOADED_FILES.append(files_[counter])
                            counter += 1
                            FILE_COUNTER = counter
                    else:
                        while counter < int(chunksize):
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
    except:
        print(str("[Errno 1] ConnectionErrot:  Error trying to connect"))
    finally:
        client.close()  # close client application
        fstp.close()    # clse fstp application
        if fstp.is_alive() is False:
            print("Successfully disconnected")

def files_to_load(file_path):
    """Load files"""
    try:
        if os.path.exists(str(file_path)):
            os.chdir(str(file_path))
            files_ = os.listdir(os.getcwd())
            return np.array(files_)
        else:
            raise  OSError("[Errno 3] OSError: Could not find path\n")
    except OSError as e:
        print(str(e))

def main():
    """main application"""
    host = ''
    port = 0000
    un = ''
    pw = ''
    chunksize = 10
    local_path = ""
    remote_path = ""
    sftp = create_sftp_connection(host=host, port=port, username=un, password=pw,
                                  local_path=local_path, remote_path=remote_path, chunksize=chunksize)

if __name__ == '__main__':
    main()





