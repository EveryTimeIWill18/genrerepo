"""
HiveSetup
=========
This module is used to connect to Hive to load in the
N-grams data into a structured data table.
"""
import os
import sys
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
        self.hive_warehouse     = None
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


            # - get the hive table information
            self.hive_tables = Popen('hive -e "use drw; show tables;"', stdout=PIPE, shell=True).communicate()
            self.hive_tables = str(list(self.hive_tables)[0]).split("\n")[:-1]

            for db in self.databases:
                if db not in self.hive_warehouse:
                    self.hive_warehouse[db] = {}
                    hive_cmd = 'hive -e "use %s; show tables;"' % db
                    pprint(hive_cmd)
                    hive_payload = Popen(hive_cmd, stdin=PIPE, shell=True).communicate()
                    hive_payload = str(list(hive_payload)[0]).split("\n")[:-1]
                    pprint(hive_payload)
                    # - create an internal dictionary to hold the table names
                    for h in hive_payload:
                        if h not in self.hive_warehouse[db]:
                            table_cmd = 'hive -e "describe %s.%s;"' %(db, h)
                            hive_table = Popen(table_cmd, stdin=PIPE, shell=True).communicate()
                            self.hive_warehouse[db][h] = list(map(lambda x: x[:2],
                                                    [str(h).split('\t') for h in str(list(hive_table)[0]) \
                                                                                                .split('\n')]))
                            self.hive_warehouse[db][h] = [[str(x).rstrip() for x in row]
                                                                for row in self.hive_warehouse[db][h] if len(row) > 1]
                            pprint(self.hive_warehouse[db][h])
        except:
            pass
        finally:
            pass


    def RunQuery(self):
        """
        Create a HiveQL query.
        :return:
        """
