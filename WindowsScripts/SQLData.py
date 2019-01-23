"""
SQLData
=======
This module connencts to sql server and loads extracts the
claim_id from the matching table. From there, the data is
joined with the corresponding file name. The pandas DataFrame
is then written to a pickle file for transferring via sftp to 
the Hadoop Server.

A few different implementations have been added in an attempt to
decrease the processing time.

"""
import os
import sys
import time
import urllib
import threading
from multiprocessing.pool import ThreadPool
from pprint import pprint

import pandas as pd
from sqlalchemy import (create_engine, MetaData,
	Table, Column, select, or_, and_, func, INT)


# - lock used  for locking I/O bound sql quieries
lock = threading.Lock()

# - pass the results of the threads
results = []

# - main module to process sql data
###################################################################################
def process_sql_data(sql_server, database, table):
	# type: (...) -> list[pd.DataFrame]
	"""
	Grab the sql data that will
	be passed to the Hadoop Server.
	"""
	# -  Setup the sql connection
	DRIVER = '{SQL Server}'
	base_connection = "driver={};" \
				"Server={};" \
				"Database={};" \
				"Trusted_Connection=yes".format(DRIVER, sql_server, database)

	# - correctly build the connection string
	connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.quote_plus(base_connection)

	# - create the engine
	engine = create_engine(connection_string)

	# - raise a connection error if connection fails
	connection  = None
	try:
		connection = engine.connect()
	except:
		print("ConnectionError: Could not connect to database.")

	# - if connected
	if connection:
		print("Successfully connected")
		
		# - sql metadata
		metadata = MetaData()
		
		# -  connect to the mapping table
		claims = Table(table, metadata, autoload=True, autoload_with=engine)

		# - grab all file extension types
		extensions = list(map(lambda x: select([claims.c.claim_id, claims.c.Files]) \
						.where(claims.c.Formats == '{}'.format(x)),
						[u'eml', u'rtf', u'msw8', u'pdf', u'html', u'docx']))
		
		# - map the executed sql script into a pandas DataFrame
		data_frames = list(map(lambda x, y: pd.DataFrame([i for i in connection.execute(x)],
					columns=['Claim Id', y]),
					extensions, [u'Eml Files', u'Rtf Files', u'Msw8 Files', 
								 u'Pdf Files' ,u'Html Files', u'Docx Files']))

		return data_frames


# - asynchronous processing of sql data
###################################################################################
def process_sql_data_async(sql_server, database, table, ext_type):
	# -  Setup the sql connection
	DRIVER = '{SQL Server}'
	base_connection = "driver={};" \
				"Server={};" \
				"Database={};" \
				"Trusted_Connection=yes".format(DRIVER, sql_server, database)

	# - correctly build the connection string
	connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.quote_plus(base_connection)

	# - create the engine
	engine = create_engine(connection_string)
	# - raise a connection error if connection fails
	connection  = None
	try:
		connection = engine.connect()
	except:
		print("ConnectionError: Could not connect to database.")

	# - if connected
	if connection:
		print("Successfully connected")

		# - sql metadata
		metadata = MetaData()

		# -  connect to the mapping table
		claims = Table(table, metadata, autoload=True, autoload_with=engine)

		# - create sql query
		query = list(map(lambda x: select([claims.c.claim_id, claims.c.Files]) \
						.where(claims.c.Formats == '{}'.format(x)),
							[ext_type]))

		# - build a pandas DataFrame
		data_frame = list(map(lambda x, y: pd.DataFrame([i for i in connection.execute(x)],
			columns=['Claim Id', y]),
			 query, [str(ext_type).upper()]))

		pprint(data_frame[0].head(10))
		print("\n")
		return data_frame

# function used with run_sql_threads, and run_threads
###################################################################################
def make_sql_connection(sql_server, database, table):
	"""
	Connect to MS-SQL Server.
	"""

	# -  Setup the sql connection
	DRIVER = '{SQL Server}'
	base_connection = "driver={};" \
				"Server={};" \
				"Database={};" \
				"Trusted_Connection=yes".format(DRIVER, sql_server, database)

	# - correctly build the connection string
	connection_string = "mssql+pyodbc:///?odbc_connect=" + urllib.quote_plus(base_connection)

	# - create the engine
	engine = create_engine(connection_string)
	# - raise a connection error if connection fails
	connection  = None
	try:
		connection = engine.connect()
	except:
		print("ConnectionError: Could not connect to database.")

	# - if connected
	if connection:
		print("Successfully connected")

		# - return the connection and claims table
		return engine, connection

###################################################################################
def run_sql_threads(ext_type, ext_name, connection, claims):

	# - lock the sql query
	lock.acquire()

	# - create sql query
	query = list(map(lambda x: select([claims.c.claim_id, claims.c.Files]) \
						.where(claims.c.Formats == '{}'.format(x)),
						[ext_type]))

	# - create a list that contains a pandas DataFrame
	df = list(map(lambda x: pd.DataFrame([i for i in connection.execute(x)], 
					columns=['Claim Id', '{}'.format(ext_name)]), query))
	
	# - release the lock
	lock.release()

	# - append the pandas DataFrame to the results list
	results.append(df[0])
	return df[0]


def run_threads():
	"""
	Run the sql queries in 
	multiple threads.
	"""

	# - static values
	sql_server = 'USTRNTPD334'
	database   = 'PCNA_Claims'
	table      = 'SA_DMS_Mapping_All_1'
	ext_types  = [u'eml', u'rtf', u'msw8', u'pdf', u'html', u'docx']
	ext_names  = [u'Eml Files', u'Rtf Files', u'Msw8 Files', u'Pdf Files' ,u'Html Files', u'Docx Files']

	# - connect to sql instance
	engine, connection = make_sql_connection(sql_server, database, table)

	# - sql metadata
	metadata = MetaData()

	# -  connect to the mapping table
	claims = Table(table, metadata, autoload=True, autoload_with=engine)

	# - create the threads
	t1 = threading.Thread(target=run_sql_threads, args=(ext_types[0], ext_names[0], connection, claims))
	t2 = threading.Thread(target=run_sql_threads, args=(ext_types[1], ext_names[1], connection, claims)) 
	t3 = threading.Thread(target=run_sql_threads, args=(ext_types[2], ext_names[2], connection, claims))
	t4 = threading.Thread(target=run_sql_threads, args=(ext_types[3], ext_names[3], connection, claims))
	t5 = threading.Thread(target=run_sql_threads, args=(ext_types[4], ext_names[4], connection, claims))
	t6 = threading.Thread(target=run_sql_threads, args=(ext_types[5], ext_names[5], connection, claims))

	# - list the threads
	threads = [t1, t2, t3, t4, t5, t6]

	# - start the threads
	t1.start()
	t2.start()
	t3.start()
	t4.start()
	t5.start()
	t6.start()

	# - join the threads
	for thread in threads:
		thread.join()


if __name__ == '__main__':
	
	# - Threaded SQL Query run
	##############################################################
	start = time.time()
	run_threads()
	total_run_time = time.time() - start
	print("Threaded SQL Query run time: {}\n".format(total_run_time))

	# - Sequential SQL Query run
	##############################################################
	sql_server, database, table = 'USTRNTPD334', 'PCNA_Claims', 'SA_DMS_Mapping_All_1'
	start = time.time()
	process_sql_data(sql_server, database, table)
	total_run_time = time.time() - start
	print("Sequential SQL Query runtime: {}".format(total_run_time))
