import os
import csv
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram
from pyspark.sql.functions import col, size, explode
from hdfs3 import HDFileSystem
import sys


#change directory to data location
os.chdir('/genre/bda/apps/claimsops/rtf_test')


# load data into a list of tuples
raw_data = []
data_list = []
row_lengths = []
with open('merged_df.csv') as sprk_file:
    for row in sprk_file:
        data_list.append(tuple(row.strip('\n').split(',',3)[1:]))
        row_lengths.append(len(row.rstrip('\n').split(',', 3)[1:]))
        raw_data.append(row)
        
        

# setup pyspark
spark = SparkSession.builder  \
                    .appName("Personal_Umbrella_eml") \
                    .getOrCreate()

# create spark dataframe
sdf = spark.createDataFrame(data_list, ['Claims_Id', 'Eml_Files', 'Extracted_Text'])

# check spark df output
sdf.show()
