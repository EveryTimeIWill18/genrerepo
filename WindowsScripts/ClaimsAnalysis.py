"""
ClaimsAnalysis
==============
Created on Wed Jan 23 2019
@author: William Murphy

This module is used to generate statistical reports containing
information about the hdfs data pipeline.
"""

import sys
import os
import time
import configparser
import pickle
import numpy as np
import pandas as pd
from pprint import pprint
from PyPDF2 import PdfFileWriter, PdfFileReader
from SystemUtils.LoadData import process_sql_data
from SystemUtils.LoadData import extract_docx_content
from SystemUtils.LoadData import df_to_pickle
from Networking.MakeSftp import transfer_data

# - Setup Pandas Options
###################################################################################
def set_pandas_options(*args, **kwargs):
	"""
	Setup for pandas options
	"""
	pass


# - Create a Joined Claims DataFrame
###################################################################################
def joined_claims_data(ini_path, df_index=5):
	"""
	Join the data from sql as well as the
	extracted text data from the .docx files.

	:return:
	"""

	# - SETUP 
	#########################################
	
	# - load the .ini config file
	config = configparser.ConfigParser()
	config.read(ini_path)
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

	# - Join the claims data frames
	#########################################
	
	# - merged df with an inner join
	inner_merged_df = sql_df.merge(docx_df, left_on='Docx Files', 
								right_on='Docx Files', how='inner',
								suffixes=('_1','_2')) \
										.drop('Claim Id_2', axis=1)

	# - merged df with an outer join
	outer_merged_df = sql_df.merge(docx_df, left_on='Docx Files', 
								right_on='Docx Files', how='outer',
								suffixes=('_1','_2')) \
										.drop('Claim Id_2', axis=1)

	# - left join
	left_merged_df = sql_df.merge(docx_df, left_on='Docx Files', 
								right_on='Docx Files', how='left',
								suffixes=('_1','_2')) \
										.drop('Claim Id_2', axis=1)

	# - get the claims files that contain null data
	null_claims_files = outer_merged_df['Docx Files'] \
							.where(outer_merged_df['Extracted Text'].isna()) \
							.dropna().tolist()
	
	# - get the claim ids that contain null data
	null_claim_ids = outer_merged_df['Claim Id_1'] \
							.where(outer_merged_df['Extracted Text'].isna()) \
							.dropna().tolist()

	# - write the outer_merged_df to a csv file
	outer_merged_df.to_csv(
		path_or_buf=os.path.join(pickle_dir,'pu_claims_docx_dataframe_1_23_2019.csv'),
		sep=str(','),
		header=True,
		encoding='utf-8'
	)

	# - return the full path to the csv file
	return os.path.join(pickle_dir,'pu_claims_docx_dataframe_1_23_2019.csv')

def run():
	"""
	Run the application.
	"""

	# - run the join
	ini_path = 'N:xxxxx\\config.ini'

	# - load the .ini config file
	config = configparser.ConfigParser()
	config.read(ini_path)
	sections = config.sections()

	host = config.get(sections[2], 'HOST')
	port = config.get(sections[2], 'PORT')
	username = config.get(sections[2], 'USERNAME')
	password = config.get(sections[2], 'PASSWORD')
	remote_path = config.get(sections[2], 'PATH')
	root = config.get(sections[1], 'PICKLE_DIR')


	csv_file = joined_claims_data(ini_path, df_index=5)
	fn = 'pu_claims_docx_dataframe_1_23_2019.csv'
	
	# - wait until the file exists to run sftp protocol
	while not os.path.exists(csv_file):
		time.sleep(1)

	if os.path.isfile(csv_file):
		transfer_data(host, port, username, password, root, fn, remote_path)

if __name__ == '__main__':
	run()
