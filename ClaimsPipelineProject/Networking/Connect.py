
from Sftp import SftpCredentials, Data__
import os
import glob
import socket
import socks
import paramiko
import zipfile
import logging
import threading
import shutil
import sshtunnel
import unicodedata
import pickle
from datetime import datetime

class SftpConnection(SftpCredentials, Data__):
    """
    SftpConnection
    --------------
    Create a secure file transfer protocol connection.
    SftpConnection inherits from the SftpCredentials
    superclass.
    """
    def __init__(self):
        SftpCredentials.__init__(self)
        Data__.__init__(self)

        self.sock   = socks.socksocket()  # current socket connection
        self.sftp   = None
        self.client = None

        # assertion variables
        # -------------------
        self.sftp_connected     = False
        self.client_connected   = False

    def connect(self, filename):
        """
        Connect to the remote server.
        :return:
        """
        try:
            # check connection credentials
            assert self.host__ is not None
            assert self.port__ is not None
            assert self.username__ is not None
            assert self.password__ is not None

            # set proxy connection values
            self.sock.set_proxy(
                proxy_type=None,
                addr=self.host__,
                port=self.port__,
                username=self.username__,
                password=self.password__
            )
            # connect the socket
            self.sock.connect((self.host__, self.port__))

            if socket.gethostname() is not None:
                print("Connection Successful:\nHost: {}".format(socket.gethostname()))
                # create transport
                self.sftp = paramiko.Transport(self.sock)

                try:
                    self.sftp.connect(
                        username=self.username__,
                        password=self.password__
                    )
                    if self.sftp.is_alive():
                        print("Transport is live.")
                        self.sftp_connected = True

                        # create client and connect
                        try:
                            self.client = paramiko.SFTPClient.from_transport(self.sftp)
                            self.client_connected = True
                            self.transport_payload(filename=filename)

                            self.client.close()
                            print("closing client")

                            self.sftp.close()
                            print("closing sftp")

                            self.sock.close()
                            print("closing socket")

                        except:
                            pass
                except:
                    pass
        except:
            pass
        finally:
         pass

    def disconnect(self):
        """
        End connection with remote server.
        :return:
        """
        try:
            if self.client_connected:
                self.client.close()
                self.client_connected = False
                print("Closing client connection")
            if self.sftp_connected:
                self.sftp.close()
                self.sftp_connected = False
                print("Closing sftp connection")
        except:
            pass
        finally:
            pass

    def transport_payload(self, filename):
        """
        Transport data to remote server.
        :return:
        """
        if self.sftp_connected and self.client_connected:
            assert self.sftp is not None
            assert self.client is not None
            assert self.local_path__ is not None
            assert self.remote_path__ is not None

            try:
                while True:
                    #payload = self.local_path__ + '\\' + filename
                    payload = filename
                    dest = self.remote_path__ + '/' + filename
                    self.client.put(
                        localpath=payload,
                        remotepath=dest
                    )
                    print("Successfully loaded {} to {}".format(filename, dest))
                    break
            except:
                print("An error occurred while transporting payload")
            finally:
                pass



