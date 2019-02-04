import os
import re
import sys
import socket
import configparser
import paramiko
import fabric
import numpy as np
import pandas as pd
from pprint import pprint

          
class HiveConfig:
    """
    HiveConfig
    ----------
    Load in the credentials
    for connecting to the hive server.
    """
    pass
                
    

class HiveConnection:
    """
    HiveConnection
    --------------
    Connect to the remote hive server.
    """
    
    def __init__(self):
        self.ssh_connection = None
        
    def make_connection(self, path_: str, config_file: str):
        """
        Connect to a remote server.
        
        :return:
        """
        try:
            if os.path.exists(path_):
                if os.path.isfile(os.path.join(path_, config_file)):
                    # - load in a configuration file
                    config = configparser.ConfigParser()
                    config.read(os.path.join(path_, config_file))
                    sections = config.sections()
                    # - import the configuration data
                    host = config.get(sections[0], 'HOST')
                    port = config.get(sections[0], 'PORT')
                    un = config.get(sections[0], 'USERNAME')
                    pw = config.get(sections[0], 'PASSWORD')
                    
                    # - create an ssh client and configuration
                    self.ssh_connection = paramiko.SSHClient()
                    self.ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    
                    # - connect to the client
                    self.ssh_connection.connect(hostname=host, username=un, password=pw)
                    if self.ssh_connection.get_transport().is_active():
                        print("Connection Successful\n---------------------\n" \
                              "Hostname:{}\n" \
                              "Full Qualified Domain:{}\n" \
                              "Address:{}".format(socket.gethostname(), 
                                                  socket.getfqdn(),
                                                  socket.gethostbyaddr(host)))
            else:
                raise OSError("OSError: Path: {} not found".format(path_))
        except OSError as e:
            print(e)
            
            
class HiveClient(HiveConnection):
    """
    HiveClient
    ----------
    Run hive remotely using hive's cli.
    """
    def __init__(self):
        HiveConnection.__init__(self)
        self.hive_warehouse = {}
        self.hive_databases = pd.DataFrame
        self.hive_tables    = np.array
    
    def execute(self, cmd_str: str):
        """
        Pass a string of commands to the 
        remote cli.
        
        :return:
        """
        stdin_, stdout_, stderr_ = self.ssh_connection.exec_command(str(cmd_str))
        return stdin_, stdout_, stderr_
    
    def close_con(self):
        if self.ssh_connection is not None:
            self.ssh_connection.close()
        else:
            print("Connection is closed")
        
            
    def generate_database_layout(self):
        """
        Create a view of the current Hive
        database layout.
        
        :return:
        """
        if self.ssh_connection is not None:
            print("Connected")
            # - Hive Cli commands to be passed to the remote linux server
            stdin_, stdout_, stderr_ = self.execute('hive -e "show databases"')
            stdout_.channel.recv_exit_status()
            
            # - create a dictionary of hive database names
            self.hive_warehouse = {d: {} 
                                   for d in list(map(lambda x: str(x).strip('\n'), 
                                                     list(stdout_.readlines())))}
            
            
            # - iterate through each key to get the table names
            for db in self.hive_warehouse:
                hive_cmd = 'hive -e "use %s; show tables;"' % str(db)
                stdin_, stdout_, stderr_ = self.execute(hive_cmd)
                stdout_.channel.recv_exit_status()
                self.hive_warehouse[db] = {d: {} for d in list(map(lambda x: str(x).strip('\n'),
                                                  list(stdout_.readlines())))}
                
            for db in self.hive_warehouse:
                for t in self.hive_warehouse[db]:
                    columns_cmd = 'hive -e "describe %s.%s;"' % (db, t)
                    stdin_, stdout_, stderr_ = self.execute(columns_cmd)
                    stdout_.channel.recv_exit_status()
                    
                    # - get the output data inot the correct form
                    self.hive_warehouse[db][t] = list(map(lambda x: re.sub(r"\s+", "", str(x)),
                                                    list(map(lambda y: y[:2],
                                                       list(map(lambda z: str(z).strip('\n').split('\t'),
                                                           list(stdout_.readlines())))))))
                    
                    # - convert to a numpy array and flatten into a single array
                    self.hive_warehouse[db][t] = np.array(self.hive_warehouse[db][t]).flatten()
                    
                    # - clean the contents of the array and break into 2 columns
                    self.hive_warehouse[db][t] = list(map(lambda x: x.lstrip("['") \
                                                          .rstrip("']") \
                                                          .split(","), 
                                                          self.hive_warehouse[db][t]))
                    
                    # - convert to a pandas DataFrame and complete further cleaning
                    #self.hive_warehouse[db][t] = pd.DataFrame(data=self.hive_warehouse[db][t], 
                    #                                          columns=["Hive Column", "Data Type", "Other"]) \
                    #                                            .applymap(lambda x: x.rstrip("'") \
                    #                                                                  .lstrip("'"))
                
           
            pprint(self.hive_warehouse)
            
        else:
            print("Not connected\ntrying to connect ...")
            
            
            
        
