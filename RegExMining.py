import re
import string
from pprint import pprint
from typing import Pattern
from functools import wraps
#
# William Murphy
# 9/19/2018
# RegExMining.py
#
# encoding pattern
encoding_pattern: Pattern[str] = re.compile(r'(\\[a-z]{1,32}[0-9]*|\{+?)')

# multiple spaces pattern
spaces_pattern: Pattern[str] = re.compile(r'\s{2,}')

# special characters pattern
special_char_pattern: Pattern[str] = re.compile(r'(\t*?\n*?\r*?\v*?\f*?)')

# punctuation pattern
punctuation_pattern: Pattern[str] = re.compile(r'(\,*?\<*?\.*?\>*?\/*?\?*?\;*?\:*?'
                                 r'\'*?\"*?\[*?\{*?\]*?\}*?\\*?\|*?\`*?'
                                 r'\~*?\@*?\#*?\$*?\%*?\^*?\&*?\**?\(*?'
                                 r'\)*?\-*?\_*?\=*?\+*?\!*?)')
# multiple capitals pattern
caps_pattern = re.compile(r'[A-Z]{2,}')

# multi-alpha-numeric patterns
multi_alpha_numeric_pattern_one = re.compile(r"[f*\d*?]")
multi_alpha_numeric_pattern_two = re.compile(r"[e{3,}\d*?]")
multi_alpha_numeric_pattern_three = re.compile(r"[b{3,}\d*?]")

regex_dict = {'encoding_pattern':                  encoding_pattern,
              'spaces_pattern':                    spaces_pattern,
              'special_char_pattern':              special_char_pattern,
              'punctuation_pattern':               punctuation_pattern,
              'multi_alpha_numeric_pattern_onw':   multi_alpha_numeric_pattern_one,
              'multi_alpha_numeric_pattern_two':   multi_alpha_numeric_pattern_two,
              'multi_alpha_numeric_pattern_three': multi_alpha_numeric_pattern_three,
              'caps_pattern': caps_pattern}

def add_regex_pattern(regex_string: str):
    def decorator(f: callable):
        @wraps(f)
        def wrapper(*args, **kwargs):
            result = None
            g = f(*args)
            regex_list = [k for k in kwargs]
            for i, _ in enumerate(regex_list):
                keyword = regex_list[i]
                if keyword in list(regex_dict.keys()):
                    pattern = regex_dict.get('{}'.format(keyword))
                    result = pattern.sub("", regex_string)
            return result
        return wrapper
    return decorator

@add_regex_pattern(regex_string=rtf_encodeing)
def set_regex_expressions(**kwargs) -> list:
    """pass regular expressions to be processed"""
    regex_list = list()
    for k in kwargs:
        if k.__str__() in list(regex_dict.keys()):
            regex_list.append(k)
    return regex_list



def main():
    r = set_regex_expressions(encoding_pattern='encoding_pattern')
    pprint(r)

if __name__ == '__main__':
    main()


