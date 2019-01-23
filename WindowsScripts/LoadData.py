"""
LoadData
========
This module connencts to sql server and loads extracts the
claim_id from the matching table. From there, the data is
joined with the corresponding file name. The pandas DataFrame
is then written to a pickle file for transferring via sftp to 
the Hadoop Server.

A few different implementations have been added in an attempt to
decrease the processing time.

"""
import sys
import os
import glob
import socket
import socks
import paramiko
import time
import urllib
import threading
import cProfile
import pstats
import multiprocessing
import dircache
import pickle
import configparser

from datetime import datetime
from xml.etree.cElementTree import XML
from pprint import pprint

import numpy as np
import pandas as pd
from pandas.compat import u
from sqlalchemy import (create_engine, MetaData,
	Table, Column, select, or_, and_, func, INT)

# - display 100 columns of data
pd.options.display.max_columns = 100

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

		# - close the sql engine connection
		connection.close()

		return data_frames


# - module to write pandas DataFrame to a pickle file
###################################################################################
def df_to_pickle(write_dir, data, project_id):
	# type (...) -> None
	"""
	Write pandas DataFrame data to
	a pickle file.
	"""

	if os.path.exists(write_dir):
		os.chdir(write_dir)
		current_date = str(datetime.today())[:10].replace('-', '_')
		save_name = project_id + 'sql_calim_id_df' + '_' + current_date + '.pickle'
		try:
			to_pkl = open(save_name, 'wb')
			pickle.dump(data, to_pkl)
			to_pkl.close()
			print("wrote pickle file to: {}".format(os.path.join(write_dir, save_name)))
		except pickle.PicklingError:
			print("PickleError: Could not pickle object")


# -  load in the requested data
###################################################################################
def load_data(source_path, ext_type):
	# type (...) -> list
	"""
	Load data from a given filepath.
	:return files:
	"""
	if os.path.exists(source_path):
		# - get all files with .ext_type
		files = list(filter(lambda x: ext_type in x, os.listdir(source_path)))
		print(len(files))
		# - join the selected files with the full file path
		files = list(map(lambda x: os.path.join(source_path, x), files))
		print("File extension: {}\nNumber of Files: {}".format(ext_type, len(files)))
		return files


# - Process Pool run: to be used with the load_data function
###################################################################################
def run_process_pool():
	# type: (...) -> list
	"""
	Create a number of pooled
	processes to run in parallel.
	"""
	root = "Z:\\WinRisk\\PC_BusinessAnalytics\\SA_Claims\\SA_2"

	# - create a pool of 3 processes
	pool = multiprocessing.Pool(processes=4)
	files = ['docx']
	results = list(map(lambda x: pool.apply(load_data, (root, x)), files))
    # - return the results list
	return results


# - extract the data from a .docx file
###################################################################################
def extract_docx_content():
	"""
	Extract text content from .docx file.
    The .docx file type is in xml format, so
    this function makes use of the 
    xml.etree.cElementTree python module.
    :return: pandas DataFrame
	"""
	# - pu claims data directory
	path_input = 'Z:\\xxxx'
	
	# -  Namespace information needed to extract content
	WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
	PARA = WORD_NAMESPACE + 'p'
	TEXT = WORD_NAMESPACE + 't'

	# - create a dict to store processed docx file
	processed_docx_files = {}
	os.chdir(path_input)

	# - collect al .docx files
	docx_files = glob.glob('*.docx')

	# - files to remove
	remove_list = []

	# - algorithm to extract the .docx data
	for docx in docx_files:
		try:
			document = zipfile.ZipFile(docx)
			xml_content = document.read('word/document.xml')
			document.close()
			tree = XML(xml_content)

			# - load data into paragraphs list
			paragraphs = []
			for paragraph in tree.getiterator(PARA):
				texts = [node.text for node in 
							paragraph.getiterator(TEXT) 
								if node.text]
				# - convert texts into a single text string
				if texts:
					paragraphs.append(' '.join(texts).lower())

			# - update the procesed_docx_files dict
			processed_docx_files.update({docx: ''.join(paragraphs)})
		except:
			print("An error occured trying to parse {}".format(docx))
			remove_list.append(docx)


# - Function that is used within the transfer_data function
###################################################################################
def load_transfer_data(root_path, file_name):
	"""
	Data to be transferred via sftp.
	"""
	if os.path.isfile(os.path.join(root_path, file_name)):
		return os.path.join(root_path, file_name)
	else:
		return None

# - make a secure transfer of data to an sftp server
###################################################################################
def transfer_data(host, port, username, password, root, fn, remote_path):
	"""
	Transer data via sftp connvection.
	"""

	# - socket setup
	sock = socks.socksocket()
	sock.set_proxy(
		proxy_type=None,
		addr=host,
		port=port,
		username=username,
		password=password
	)

	# - connect the socket
	sock.connect((host, port))
	print("Host Name\n-------------")
	print(socket.gethostname())
	print('\n')
	print("Full Host Name\n--------------")
	print(socket.getfqdn())
	print("Host address\n------------")
	print(socket.gethostbyaddr(host))

	# - create sftp transport
	sftp = paramiko.Transport(sock)
	# - connect
	sftp.connect(
		username=username,
		password=password
	)
	if sftp.is_alive():
		print("connection is live")
		# - create client
		client = paramiko.SFTPClient.from_transport(sftp)

		# - load the data to be transferred
		data = load_data(root, fn)
		if data is not None:
			client.put(localpath=str(data), 
				remotepath=os.path.join(remote_path, fn))
		print("Finished transferring payload")

	# - close the connection
	client.close()
	sftp.close()

# - join the two different pandas DataFrames
###################################################################################
def join_claims_data(df_index=5):
	"""
	Join the data from sql as well as the
	extracted text data from the .docx files.
	"""
	# - ms sql server variables
	sql_server = ''
	database   = ''
	table      = ''

	# - pickle variables
	root_path = 'N:\\xxxxx'

	pkl_file = 'PU_Claimssql_calim_id_df_2019_01_22.pickle'

	# - load the pandas DataFrames
	sql_df  = process_sql_data(sql_server, database, table)[df_index]
	docx_df = pickle.load(open(os.path.join(root_path, pkl_file)))

	# - check output
	#print(sql_df.head())
	#print(docx_df.head())

	# - merged df with an inner join
	inner_merged_df = sql_df.merge(docx_df, left_on='Docx Files', 
								right_on='Docx Files', how='inner',
								suffixes=('_1','_2'))
	
	# - merged df with an outer join
	outer_merged_df = sql_df.merge(docx_df, left_on='Docx Files', 
								right_on='Docx Files', how='outer')

	# - join the data frames
	inner_joined_df = sql_df.join(docx_df, how='inner', lsuffix='_1', rsuffix='_2')
	outer_joined_df = sql_df.join(docx_df, how='outer', lsuffix='_1', rsuffix='_2')
	
	# - concatinate the data frames
	inner_concat_df = pd.concat([sql_df, docx_df], join='inner', axis=0)
	outer_concat_df = pd.concat([sql_df, docx_df], join='outer', axis=0)


	#print(outer_merged_df.head())
	#print(inner_merged_df)

	# - test out the data
	#s1 = outer_merged_df.loc[0, :] 				# row 0, all columns
	#df1 = outer_merged_df.loc[[0, 1, 2], :]		# first 3 rows, all columns
	#df2 = outer_merged_df.loc[0:5, :]			# first 5 rows, all columns
	

	# - get the docx files that have Extracted text == NaN
	nulls = outer_merged_df['Docx Files'] \
								.where(outer_merged_df['Extracted Text'].isna()) \
								.dropna().tolist()

	final_df = inner_merged_df.drop('Claim Id_2', axis=1)
	
	final_df.to_csv(
			path_or_buf=os.path.join(root_path, 'test_dataframe_1_22_2019.csv'),
			sep=str(','),
			header=True,
			encoding='utf-8'
	)

	# - write the inner merged df to a pickle file
	#df_to_pickle(root_path, inner_merged_df, 'PU_Claims_Merged_DF')

	# - file transfer variables
	host = ''
	port = ''
	username = ''
	password = ''
	remote_path = ''
	fn = 'PU_Claims_Merged_DFsql_calim_id_df_2019_01_22.pickle'
	
	# - load the new pndas DataFrame to the Hadoop server
	#transfer_data(host, port, username, password, root_path, fn, remote_path)
	
# - Setup pandas DataFrames for Statistical Analysis
###################################################################################						
def report_analysis(ini_file, df_index=5):
	"""
	Generate a report of statistical calculations.
	Task: Create a pandas DataFrame containing the 
	claim id, file name, and extracted text
	
	:return: 
	"""

	# - load the .ini config file
	config = configparser.ConfigParser()
	config.read(os.path.join(os.getcwd(), ini_file))
	sections = config.sections()
	
	# - sql setup
	sql_server = config.get(sections[0], 'DB_SERVER')
	database   = config.get(sections[0], 'DB_NAME')
	table      = config.get(sections[0], 'TABLE')

	# - pickle setup
	pickle_dir = config.get(sections[1], 'PICKLE_DIR')
	pkl_file   = 'PU_Claimssql_calim_id_df_2019_01_22.pickle'

	# - load the pandas DataFrames
	sql_df  = process_sql_data(sql_server, database, table)[df_index]
	docx_df = pickle.load(open(os.path.join(pickle_dir, pkl_file)))

	# - DataFrame Options Setup
	######################################
	pd.options.display.max_rows 	= 200
	pd.options.display.max_columns = 10
	pd.set_option("display.precision", 5)

	# - DataFrame Information
	#######################################
	print("SQL DataFrame\n=============")
	sql_df.info(
		verbose=True,		# show full summary
		buf=None,			# where to send output(default: sys.stdout)
		max_cols=10,		# when to switch from verbose to truncated output
		memory_usage=True,  # display tot memory usage
		null_counts=True    # show null counts
	)
	print("\n")
	print("DOCX DataFrame\n=============")
	docx_df.info(
		verbose=True,		# show full summary
		buf=None,			# where to send output(default: sys.stdout)
		max_cols=10,		# when to switch from verbose to truncated output
		memory_usage=True,  # display tot memory usage
		null_counts=True    # show null counts
	)
	print("\n")



if __name__ == '__main__':
	report_analysis('config.ini')
