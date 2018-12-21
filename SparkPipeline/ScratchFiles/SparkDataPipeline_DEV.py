import os
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram
from pyspark.sql.functions import size, explode, Column
from hdfs3 import HDFileSystem
import sys
from functools import wraps, reduce, partial
import inspect
from itertools import chain

# --- Set Current Directory to csv directory
os.chdir('/genre/bda/apps/claimsops/local_git/Project_Data')
raw_files = os.listdir(os.getcwd())


# --- load data into a list of tuples
raw_data = []
data_list = []
row_lengths = []


def load(filename):
    """
    Load in the csv file
    :param filename:
    :return:
    """
    try:
        if filename in raw_files:
            with open(filename, 'r') as spark_file:
                row_cntr = 0
                for row in spark_file:
                    try:
                        data_list.append(row.strip('\n').split(',', 3)[1:])
                        row_lengths.append(len(row.strip('\n').split(',', 3)[1:]))
                        raw_data.append(row)
                        row_cntr += 1
                    except:
                        print("An error occurred loading row {}".format(row_cntr))
                # --- reset the column names
                data_list[0] = 'Claim_Id', 'filename', 'raw_wordlist'
        else:
            raise OSError("File: {} does not exists in current directory".format(filename))
    except OSError as e:
        print e
    finally:
        print("finished processing csv file: {}".format(filename))


def deploy_spark_session(func):
    @wraps(func)
    def create_spark_session(session_name):
        """
        Create a spark session
        :param session_name:
        :return:
        """
        try:
            spark = SparkSession.builder \
                                .appName('{}'.format(session_name)) \
                                .getOrCreate()
            f = func(session_name)
            return f
        except:
            print("CreateSparkSessionError: An error occurred in creating the spark session")
    return create_spark_session


class SparkPipeline(object):
    """
    SparkPipeline
    =============
    Create and deploy  Spark pipeline
    """

    def __init__(self):
        self.tasks  = []
        self.params = []
        self.func_info = dict()


    def task(self, depends_on=None):
        idx = 0
        if depends_on:
            idx = self.tasks.index(depends_on) + 1
        def wrapper(f):
            print("Adding {} to the pipeline".format(f.__name__))
            #list(chain.from_iterable([i for i in inspect.getargspec(task_two) if i != None]))
            func_params = list(chain.from_iterable([i for i in inspect.getargspec(f)
                                                    if i != None]))
            if len(func_params) > 0:
                self.func_info.update({f.__name__: func_params})
            self.tasks.insert(idx, f)
            return f
        return wrapper

    def run_pipeline(self, input_):
        """
        run_pipeline
        ------------
        Run the spark data pipeline.
        :param input_:
        :return:
        """
        output = input_
        counter = 0
        for task in self.tasks:
            if counter == 0:
                output = task(output)
            elif counter > 0:
                pass
        return output

    def run(self):
        """
        run
        ---
        Runs the spark pipeline.
        :return:
        """
        output_ = None
        params_ = None
        idx = 0
        for key in list(self.func_info.keys()):
            params = self.func_info.get('{}'.format(key))
            if len(params) > 0:
                if len(params) == 1:
                    # don't apply partial
                    output_ = self.tasks[idx](params[0])
                    pprint(output_)
                if len(params) > 1:
                    pass    # apply partial
                    

# --- Testing out the Spark Data Pipeline
spark_pipeline = SparkPipeline()

@spark_pipeline.task()
def task_one(path_):
    """
    task_one
    --------
    Load Data into the spark pipeline.
    :param filename:
    :return:
    """
    try:
        if os.path.exists(path_):
            os.chdir(path_)
            return os.listdir(os.getcwd())
        else:
            raise OSError("OSError: Path: {} not found".format(path_))
    except OSError as e:
        print(e)
    finally:
        print("Current Directory\n------------------\n{}".format(os.getcwd()))

@spark_pipeline.task(depends_on=task_one)
def task_two(filename):
    """
    task_two
    --------
    Load data into lists.
    :return:
    """
    print("Running task_two")
    #params = list(chain.from_iterable(inspect.getargspec([i for i in inspect.getargspec(task_two) if i is not None])))
    params = list(inspect.getargspec(task_two))
    p = list(chain.from_iterable([i for i in params if i is not None]))
    param_values = {}
    if len(p) > 0:
        for i, v in enumerate(p):
            try:
                value = raw_input("Please enter a value for {} ==> ".format(v))
                param_values.update({v: value})
            except:
                pass
    if param_values.get(p[0]) in os.listdir(os.getcwd()):
        print"File: {} exists in current directory.".format(p[0])
        filename = param_values.get(p[0])
        with open(filename, 'r') as spark_file:
            for row in spark_file:
                DATA_LIST.append(row.strip('\n').split(',', 3)[1:])
                ROW_LENGTHS.append(len(row.strip('\n').split(',', 3)[1:]))
                RAW_DATA.append(row)
        claim_id = 'Claim_Id'
        filenames = 'filename'
        word_list = 'raw_wordlist'
        DATA_LIST[0] = claim_id, filenames, word_list
        pprint(DATA_LIST[:5])
    else:
        pass

@spark_pipeline.task(depends_on=task_two)
def task_three(app_name):
    """
    Create a new spark session
    :param app_name:
    :return:
    """
    params = list(inspect.getargspec(task_three))
    p = list(chain.from_iterable([i for i in params if i is not None]))
    param_values = {}
    if len(p) > 0:
        for i, v in enumerate(p):
            try:
                value = raw_input("Please enter a value for {} ==> ".format(v))
                param_values.update({v: value})
            except:
                pass
    app_name = param_values.get(p[0])


    SPARK = SparkSession.builder \
                        .appName(app_name) \
                        .getOrCreate()

    # --- create spark dataframe
    SPARK_DATAFRAME = SPARK.createDataFrame(DATA_LIST[1:], ['Claim_Id', 'filename', 'raw_wordlist'])

    # --- setup tokenizer
    TOKENIZER = Tokenizer(inputCol='raw_wordlist', outputCol="inter_wordlist")

    # --- create output vector
    VECTOR_DATAFRAME = TOKENIZER.transform(SPARK_DATAFRAME) \
                                .select(["Claim_Id", "filename", "inter_wordlist"])

@spark_pipeline.task(depends_on=task_three)
def task_four(ngram):
    """
    Set the ngram value
    :param ngram:
    :return:
    """
    params = list(inspect.getargspec(task_four))
    p = list(chain.from_iterable([i for i in params if i is not None]))
    param_values = {}
    if len(p) > 0:
        for i, v in enumerate(p):
            try:
                value = raw_input("Please enter a value for {} ==> ".format(v))
                param_values.update({v: value})
            except:
                pass
    ngram = param_values.get(p[0])

    if int(ngram) == 2:
        # --- list of stopwords
        stopwords = {'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself',
                     'yourselves', 'he', 'him', 'his', 'himself'
            , 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
                     'what', 'which', 'who', 'whom', 'this', 'that'
            , 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having',
                     'do', 'does', 'did', 'doing', 'an', 'the', 'and'
            , 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against',
                     'between', 'into', 'through', 'during'
            , 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over',
                     'under',
                     'again', 'further', 'then', 'once'
            , 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most',
                     'other',
                     'some', 'such', 'no', 'nor', 'not'
            , 'only', 'own', 'same', 'so', 'than', 'too', 'very', 'can', 'will', 'just', 'don', 'should', 'now', ' a ',
                     'insured', 'sured', 'coverage',
                     'year', 'dob', 'insd', 'left'}

        # --- remove stop words
        REMOVER = StopWordsRemover()
        stopwords = REMOVER.getStopWords()
        REMOVER.setInputCol("inter_wordlist")
        REMOVER.setOutputCol("inter_wordlist_two")

        stpwrds_rmvd_sdf = REMOVER.transform(VECTOR_DATAFRAME) \
                                    .select(["Claim_Id", "filename", "inter_wordlist_two"])


    else:
        pass



spark_pipeline.run_pipeline(input_='')


