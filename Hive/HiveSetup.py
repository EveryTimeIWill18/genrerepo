"""
HiveSetup
=========
This module is used to connect to Hive to load in the
N-grams data into a structured data table.
"""
import os
import sys
from itertools import chain
from subprocess import (PIPE, Popen, call, check_output,
                        check_call, CalledProcessError)
from pprint import pprint
import numpy as np
import pandas as pd


class Hive:
    """
    Hive
    ----
    Hive command API.
    Command Line Options(CLI):
        -H,--help: Print help information
        --define, -d: variable substitution(e.g. -d A=B)
        -e <quoted-query-string>: SQL from command line
        -f <filename>: SQL from files
        -h <hostname>: connecting to Hive Server on remote host
            --hiveconf <property=value>: Use value for given property
            --hivevar <key=value>: Variable substitution to apply to hive
        -i <filename>: Initialization SQL file
        -p <port>: connecting to Hive Server on port number
        -S,--silent: Silent mode in interactive shell
        -v,--verbose: Verbose mode (echo executed SQL to the console)

        #payload = list(map(lambda x: 'hive -e "use {}; show tables;'.format(x), self.databases))
    """


    def __init__(self):
        self.hive_warehouse     = {}
        self.hive_tables        = None
        self.current_database   = None
        self.databases          = None
        self.tables             = None
        self.current_table      = None

        # - attempt to load in current Hive information
        try:
            # -  get the hive database information
            self.databases = Popen('hive -e "show databases;"', stdout=PIPE, shell=True).communicate()
            self.databases = str(list(self.databases)[0]).split("\n")[:-1]
            for db in self.databases:
                pprint("Database: {}".format(db))
                if db not in self.hive_warehouse:
                    self.hive_warehouse[db] = {}
                    hive_cmd = 'hive -e "use %s; show tables;"' % db
                    hive_payload = Popen(hive_cmd, stdout=PIPE, shell=True).communicate()
                    hive_payload = str(list(hive_payload)[0]).split("\n")[:-1]
                    pprint("Hive Payload: {}".format(hive_payload))
                    # - create an internal dictionary to hold the table names
                    for h in hive_payload:
                        if h not in self.hive_warehouse[db]:
                            self.hive_warehouse[db][h] = []
                            columns_cmd = 'hive -e "describe %s.%s;"' % (db, h)
                            columns_payload = Popen(columns_cmd, stdout=PIPE, shell=True).communicate()
                            columns_payload =  list(filter(lambda x: len(x) >= 3,
                                                    [str(h).split('\t')
                                                     for h in str(list(columns_payload)[0]).split('\n')]))
                            columns_payload = list(map(lambda x: x[:2],
                                                       [[str(x).rstrip() for x in row]
                                                        for row in columns_payload if len(row) > 1]))
                            # - update the list
                            self.hive_warehouse[db][h] = columns_payload
        except:
            pass
        finally:
            pass

    def run_query(self):
        """
        Run the initial hive query to
        create the hive database mapping file.
        :return:
        """
        pass





hive_context_one = Hive()
pprint(hive_context_one.hive_warehouse)

#database_payload = 'hive -e "show databases;"'
#hive_databases = Popen(database_payload, stdin=PIPE, stdout=PIPE, shell=True).communicate()
#hive_databases = str(list(hive_databases)[0]).split("\n")[:-1]
#print("Hive Databases\n============\n{}".format(hive_databases))




# - this works to get each table name
#table_payload = 'hive -e "describe %s.%s;"' % (hive_databases[4], 'sa_dms_mapping')
#pprint(table_payload)
#hive_table = Popen(table_payload, stdin=PIPE, stdout=PIPE, shell=True).communicate()
#hive_table = list(filter(lambda x: len(x) >= 3, [str(h).split('\t') for h in str(list(hive_table)[0]).split('\n')]))
#hive_table = list(map(lambda x: x[:2], [[str(x).rstrip() for x in row] for row in hive_table if len(row) > 1]))
#headers = ['Column Name', 'Data Type']
#hive_df = pd.DataFrame(hive_table, columns=headers)
#print(hive_df)

#hive_table = [[str(x).rstrip() for x in row] for row in hive_table if len(row) > 1]



#hive_table = [str(h).split('\t') for h in str(list(hive_table)[0]).split('\n')]
#hive_table = list(chain.from_iterable(list(map(lambda x: x[:2],
#                    [str(h).split('\t') for h in str(list(hive_table)[0]).split('\n')]))))
#hive_table =  list(filter(lambda x: len(x)>=3 , [str(h).split('\t') for h in str(list(hive_table)[0]).split('\n')]))
#print("Hive Tables\n========\n{}".format(hive_table))
#hive_table = list(map(lambda x: x[:2], [str(h).split('\t') for h in str(list(hive_table)[0]).split('\n')]))
#hive_table = [[str(x).rstrip() for x in row] for row in hive_table if len(row) > 1]

