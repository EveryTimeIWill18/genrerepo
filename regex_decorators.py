import os
import sys
import re
import mmap
from functools import wraps, reduce
import glob
from pprint import pprint
from RegExMining import *
from RegExMining import add_regex_pattern
import numpy as np
import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB


# ----------------------------------------------------------------
# decorator pipeline for data pre-processing

def get_file_type(f: callable):
    """decorator to import the different
    data types into python"""
    @wraps(f)
    def wrapper(path: str, ftype: str):
        """a wrapper function"""
        try:
            if type(ftype) == str:
                g = f(path, ftype)
                os.chdir(g[0])
                return glob.glob('*{}'.format(g[1]))
            else:
                raise TypeError("warning! attribute is not of type str")
        except TypeError as e:
            return e
    return wrapper


def open_data_file(f: callable):
    """read in data for pre-processing"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        """a wrapper function"""
        g = f(*args, **kwargs)
        with open(g[0], 'rb') as file:
            CURRENT_FILE = str(g)
            mm_file = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
            raw_data = mm_file.read().decode('utf-8')
            return raw_data
    return wrapper

def apply_regex_exps(**regex_patterns):
    def decorator(f: callable):
        def wrapper(*args, **kwargs):
            g = f(*args, **kwargs)
            regex_list = [k for k in regex_patterns]
            for i, _ in enumerate(regex_list):
                keyword = regex_list[i]
                if keyword in list(regex_dict.keys()):
                    pattern = regex_dict.get('{}'.format(keyword))
                    global result
                    result = pattern.sub("", g)
            return result
        return wrapper
    return decorator
# ----------------------------------------------------------------
## TEST DECORATOR: NOT TO BE USED IN PRODUCTION ##
def preprocesses_data(f: callable):
    @wraps(f)
    def wrapper(*args, **kwargs):
        g = f(*args, **kwargs)
        result = encoding_pattern.sub(" ", g)
        result = spaces_pattern.sub(" ", result)
        result = special_char_pattern.sub("", result)
        result = punctuation_pattern.sub("", result)
        result = multi_alpha_numeric_pattern_one.sub("", result)
        result = multi_alpha_numeric_pattern_two.sub("", result)
        result = multi_alpha_numeric_pattern_three.sub("", result)
        result = caps_pattern.sub("", result)
        return result
    return wrapper


#@apply_regex_exps(encoding_pattern='encoding_pattern',
#                  spaces_pattern='spaces_pattern',
#                  special_char_pattern='special_char_pattern',
#                  punctuation_pattern='punctuation_pattern',
#                 multi_alpha_numeric_pattern_one='multi_alpha_numeric_pattern_one',
#                  multi_alpha_numeric_pattern_two='multi_alpha_numeric_pattern_two',
#                  multi_alpha_numeric_pattern_three='multi_alpha_numeric_pattern_three',
#                  caps_pattern='caps_pattern'
#                  )
@preprocesses_data
@open_data_file
@get_file_type
def read_data(path: str, ftype: str) -> tuple:
    """Read data types"""
    p = path.__str__()
    file_type = ".{}".format(ftype)
    return (p, file_type)







def main():
    project_path = "FILEPATH GOES HERE"

    rtf_list = read_data(project_path, 'rtf')
    pprint(rtf_list)





if __name__ == '__main__':
