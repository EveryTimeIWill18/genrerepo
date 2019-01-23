# -*- coding: utf-8 -*-
import os
import csv
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram
from pyspark.sql.functions import size, explode, col
from pyspark.sql.functions import _functions
from hdfs3 import HDFileSystem
import sys
from datetime import datetime

# - Function creates the N-Grams that will be loaded into Hive
###################################################################################
def Run_N_Grams(root_dir, file_name, n, app_name, extension_type, project_name):
    # type: (...) -> None
    """
    Run the N-Grams language frequency algorithm.

    :param root_dir:        the root directory
    :param file_name:       the name of the csv file to be loaded
    :param n:               the non-negative integer value for generating the N-Grams
    :param app_name:        the name of the spark application
    :param extension_type:  the file extension[.eml, .pdf, .doc, .docx, .rtf]
    :param project_name:    the name of the project[SA_Claims, Personal_Umbrella]

    :return: None
    """
    if os.getcwd() is not root_dir:
        print('changing directory: {}'.format(root_dir))
        os.chdir(root_dir)


    # - list data containers
    raw_data    = []    # holds the raw text data
    data_list   = []    # holds the parsed text data
    row_lengths = []    # stores the row lengths

    # - read the csv file using the 'with' context manager
    if os.path.isfile(os.path.join(root_dir, file_name)):
        with open(os.path.join(root_dir, file_name)) as spark_file:
            for row in spark_file:
                data_list.append(tuple(row.strip('\n').split(',', 3)[1:]))
                row_lengths.append(len(row.rstrip('\n').split(',', 3)[1:]))
                raw_data.append(row)

    # - reset the column names
    claim_id    = 'Claim_Id'
    filenames   = 'filename'
    word_list   = 'raw_wordlist'

    # - rename the columns in data_list
    data_list[0] = claim_id, filenames, word_list

    # - check to make sure row_length consists only of the value 3
    try:
        if len(set(row_lengths)) == 1:
            print("Data successfully loaded")
        if len(set(row_lengths)) > 1:
            raise Exception("LoadDataException: Data not transformed properly.")
    except Exception as e:
        print(e)

    # - create a spark session
    spark = SparkSession.builder \
                        .appName(app_name) \
                        .getOrCreate()

    # - create a spark data frame
    sdf = spark.createDataFrame(data_list[1:], list(map(lambda x: str(x), [claim_id, filenames, word_list])))

    # - setup tokenizer
    tokenizer = Tokenizer(inputCol='raw_wordlist', outputCol="inter_wordlist")

    # - create an output vector
    vector_df = tokenizer.transform(sdf).select(["Claim_Id", "filename", "inter_wordlist"])

    # - stop words to be removed
    remover = StopWordsRemover()
    stopwords = remover.getStopWords()
    stopwords = {'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself',
                 'yourselves', 'he', 'him', 'his', 'himself'
        , 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
                 'what', 'which', 'who', 'whom', 'this', 'that'
        , 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having',
                 'do', 'does', 'did', 'doing', 'an', 'the', 'and'
        , 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against',
                 'between', 'into', 'through', 'during'
        , 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under',
                 'again', 'further', 'then', 'once'
        , 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other',
                 'some', 'such', 'no', 'nor', 'not'
        , 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will', 'just', 'don', 'should', 'now', ' a ',
                 'insured', 'sured', 'coverage',
                 'year', 'dob', 'insd', 'left'}

    # - create the N-gram
    ngram = NGram(n=n, inputCol="inter_wordlist", outputCol="wordlist")
    dev_sdf = ngram.transform(vector_df)
    dev_sdf = dev_sdf.where(size(col("wordlist")) >= n)
    filtered_sdf = dev_sdf.select("Claim_Id", "filename", explode(dev_sdf.inter_wordlist).alias('wordlist'))

    # - save to hive: Update the save name
    hdfs_location = '/bda/claimsops/data/'
    current_date  = str(datetime.today())[:10].replace('-', '_')
    n_gram_str = ''
    if n == 2:
        n_gram_str = 'BiGrams'
    else:
        n_gram_str = str(n).upper() + 'Grams'

    hive_str = hdfs_location + project_name + '/' + extension_type + '_' + n_gram_str + '_' + current_date
    print(hive_str)

    #filtered_sdf.write.format('orc').save('')
