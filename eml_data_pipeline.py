
import os
import mmap
import glob
import email
import re
import numpy as np
import pandas as pd
from functools import wraps
from pprint import pprint
import email.iterators
import string
import base64

test_path = "pathname"
os.chdir(test_path)
g = glob.glob('*.eml')


def load_email_data(chunk):
    email_dict = dict()
    counter = 0
    
    while counter < chunk:
        email_dict.update({str(g[counter]): []})
        msg = open(g[counter], 'rb')
        e = email.message_from_file(msg)
        metadata = [e['To'], e['From'], e['Subject'], e['Date']]
        for i, _ in enumerate(metadata):
            current = metadata[i]
            if current is not None:
              email_dict.get(list(email_dict.keys())[counter]).append(current)
            else:
              email_dict.get(list(email_dict.keys())[counter]).append('N/A')
        
        cleaned_data = extract_data(e)
        email_dict.get(list(email_dict.keys())[counter]).append(cleaned_data)
              
        msg.close()
        counter += 1
    return email_dict


def extract_data(data):

    # regex to clean raw email file
    tags = re.compile(r'\<.*?\>')
    spaces = re.compile(r'\s+')
    email_contents = email.iterators.body_line_iterator(data, decode=True)

    # extract components
    extracted_contents = []
    for c in email_contents:
        extracted_contents.append(c)

    # extract email message body
    raw_data = ''.join(extracted_contents)
    front = raw_data.find('<html>')
    back = raw_data.find('</html>')
    subset_raw_data = raw_data[front:back]

    # clean content with regex
    subset_raw_data = tags.sub('<>', subset_raw_data)
    parsed_result = ''.join(subset_raw_data)
    parsed_result = parsed_result.translate(
                                string.maketrans(" ", " "),
                                string.punctuation
                            ).lower()
    parsed_result = spaces.sub(' ', parsed_result)

    # final output   
    final_result = ''.join(parsed_result)

    return final_result
    
    
def convert_to_dataframe(data):
    max_length = 0
    for i, _ in enumerate(list(data.keys())):
        key = list(data.keys())[i]
        if len(data.get(key)) > max_length:
            max_length = len(data.get(key))

    pprint(max_length)
    
    for i, _ in enumerate(list(data.keys())):
        key = list(data.keys())[i]
        if len(data.get(key)) < max_length:
            current_len = len(data.get(key))
            while current_len < max_length:
                data.get(list(data.keys())[i]).append('N/A')
                current_len += 1

    return data
            
    
        
    

def main():
    load = load_email_data(chunk=10)
    convert_to_dataframe(load)


if __name__ == '__main__':
    main()
