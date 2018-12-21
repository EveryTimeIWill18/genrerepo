from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData, Table
from sqlalchemy import (create_engine, MetaData,
                        Table, Column, select, or_, and_, func, INT)

from sqlalchemy.orm import sessionmaker
from functools import wraps, reduce
from urlparse import urlparse
import pydoc
import urllib
from pprint import pprint
from functools import wraps
import numpy as np
import pandas as pd
import os
import pickle
import socket
import socks
import paramiko
from sftp_transfer import *
from datetime import datetime
connection = None

def set_table(table_name):
    """create sql alchemy query"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            engine = func(*args, **kwargs)
            metadata = MetaData()
            tables = np.array(engine.table_names())
            pprint(tables) # remove this
            if str(table_name) in tables:
                return Table(str(table_name), metadata, autoload=True, autoload_with=engine)
            else:
                return "Table name not found"
        return wrapper
    return decorator


@set_table(table_name='Personal_Umbrella_DMS_Mapping_4')
def build_sql_engine(server, database, trusted=True):
    """create the sql engine"""
    global connection
    driver = '{SQL Server}'
    trust = "yes" if trusted else "no"
  
    base_connection = "driver={};Server={};Database={};Trusted_Connection={}".format(driver, server, database, trust)
    connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.quote_plus(base_connection)

    try:
        engine = create_engine(connection_string)
        connection = engine.connect()
    except:
        return "Failed to create sql engine"
    return engine

@set_table(table_name='SA_DMS_Mapping1')
def build_sql_engine_sa_claims(server, database, trusted=True):
    """create the sql engine"""
    global connection
    driver = '{SQL Server}'
    trust = "yes" if trusted else "no"

    base_connection = "driver={};Server={};Database={};Trusted_Connection={}".format(driver, server, database, trust)
    connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.quote_plus(base_connection)

    try:
        engine = create_engine(connection_string)
        connection = engine.connect()
    except:
        return "Failed to create sql engine"
    return engine



def run_query(srvr, db, t):
    """run sql query"""
    global connection
    auto_bi = build_sql_engine(srvr, db, t) # func, takes server, database, trustd

    # build query(add formats from the table)
    stmt = select([auto_bi.c.claim_id, auto_bi.c.files]) \
                        .where(auto_bi.c.Files.like('%.eml%'))
    results = connection.execute(stmt).fetchall()

    # extract query data
    index = []
    filename = []

    for result in results:
        r = result
        index.append(r[0].__str__())
        filename.append(r[1])
    connection.close()  # close sql connection

    index = np.array(index)
    filename = np.array(filename)
    #s = pd.Series(data=index, index=filename, name="Claim_Id")
    sdf = pd.DataFrame(data=index, index=filename, columns=["Eml_files"])
    return sdf

def create_query(srvr, db, t, extension, df_name):
    """create and run sql query"""
    global connection
    auto_bi = build_sql_engine(srvr, db, t) # func, takes server, database, trustd
    # build query(add formats from the table)
    stmt = select([auto_bi.c.claim_id, auto_bi.c.files]) \
                        .where(auto_bi.c.files.like('%.{}%'.format(extension)))
    results = connection.execute(stmt).fetchall()
    pprint(results)

    # extract query data
    index = []
    filename = []

    # load data into lists
    for result in results:
        r = result
        index.append(r[0].__str__())
        filename.append(r[1])
    connection.close()  # close sql connection

    # create DataFrame
    index = np.array(index)
    filename = np.array(filename)
    df_dict = {'Rtf_Files': filename, 'Claims_Id': index}

    sdf = pd.DataFrame(data=df_dict)
    return sdf


def create_query_sa_claims(srvr, db, t, extension, df_name):
    """create and run sql query"""
    global connection
    sa_claim = build_sql_engine_sa_claims(srvr, db, t) # func, takes server, database, trustd
    # build query(add formats from the table)
    stmt = select([sa_claim.c.claim_id, sa_claim.c.Files]) \
                        .where(sa_claim.c.Files.like('%.{}%'.format(extension)))
    results = connection.execute(stmt).fetchall()
    pprint(results)

    # extract query data
    index = []
    filename = []

    # load data into lists
    for result in results:
        r = result
        index.append(r[0].__str__())
        filename.append(r[1])
    connection.close()  # close sql connection

    # create DataFrame
    index = np.array(index)
    filename = np.array(filename)
    df_dict = {'Rtf_Files': filename, 'Claims_Id': index}

    sdf = pd.DataFrame(data=df_dict)
    return sdf


def to_pickle_file(data_, pkl_path, save_name):
    """convert to pickle file"""
    EXIT_SUCCESS = 1
    try:
        if os.path.exists(str(pkl_path)):
            try:
                save_ = str(pkl_path) + '\\' + str(save_name)+ str(datetime.today())[:10] + ".pickle"
                output = open(save_, "wb")
                pickle.dump(data_, output)
                print("finished creation of pickle file")
            except pickle.PicklingError:
                EXIT_SUCCESS = -1 # failure creating pickle file
                print("[PicklingError]: Could not pickle obj")
        else:
            raise OSError("[OSError]: Path not found")
    except OSError as e:
        print(str(e))
    finally:
        output.close()
        print("finished pickling process with EXIT_SUCCESS:{}".format(EXIT_SUCCESS))
