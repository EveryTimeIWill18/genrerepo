import os
import email
import email.iterators
from glob import glob
from pprint import pprint
from functools import wraps
import string
import mmap
import pandas as pd


# --- globals
EMAIL_COUNTER = 0    
CURRENT_EMAIL = None
CURRENT_EMAIL_NAME = ''
EML_LIST = glob('*.eml')
EMAIL_DICT = dict()


def to_dataframe(a_dict):
    """convert dictionary to dataframe"""
    df = pd.DataFrame(data=a_dict)
    print(df.head())
    



def extract_email_contents(write_to_file=False, filename=None):
    """Extracts and cleans the current .eml and writes to file if true"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global CURRENT_EMAIL, EMAIL_DICT, CURRENT_EMAIL_NAME, EMAIL_COUNTER
            f = func(*args, **kwargs)

            # regex to clean raw email file
            tags = re.compile(r'\<.*?\>')
            spaces = re.compile(r'\s+')

            # iterate through the email amd decode to utf-8
            email_contents = email \
                                 .iterators \
                                 .body_line_iterator(CURRENT_EMAIL, decode=True)

            # extract all content from current email
            extracted_contents = []
            for content in email_contents:
                extracted_contents.append(content)

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
            EMAIL_DICT.get('{}'.format(CURRENT_EMAIL_NAME)).append(final_result)

            # TEST: to be removed
            FINAL_CONTENT = ''.join(EMAIL_DICT.get('{}'.format(CURRENT_EMAIL_NAME)))
            #pprint(FINAL_CONTENT)

            # call write function
            if write_to_file:
                write_contents_to_file(filename)

            EMAIL_COUNTER += 1
            
            return f
        return wrapper
    return decorator
                

            
           
def write_contents_to_file(filename):
    """write cleaned email to file"""
    global EMAIL_DICT, CURRENT_EMAIL_NAME

    output_name = filename + '\\' + CURRENT_EMAIL_NAME + ".txt"
    output_contents =  EMAIL_DICT.get('{}'.format(CURRENT_EMAIL_NAME))[1]
    w = open(output_name, 'w')
    w.write(output_contents)
    w.close()
    
        
def get_email_meta_data(to_=True, from_=True, subject_=True, date_=True):
    """Extract email metadata"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global CURRENT_EMAIL_NAME, EMAIL_DICT
            f = func(*args, **kwargs)
            metadata = []
            print("current file", )
            #  metadata to be optionally added as needd
            if to_:
                to__ = CURRENT_EMAIL['To']
                if to__ is not None:
                    metadata.append(to__)
                else:
                    to__ = ''
                    metadata.append(to__)
            if from_:
                from__ = CURRENT_EMAIL['From']
                if from__ is not None:
                    metadata.append(from__)
                else:
                    from__ = ''
                    metadata.append(from__)
            if subject_:
                subject__ = CURRENT_EMAIL['Subject']
                if subject__ is not None:
                    metadata.append(subject__)
                else:
                    subject__ = ''
                    metadata.append(subject__)
            if date_:
                date__ = CURRENT_EMAIL['Date']
                if date__ is not None:
                    metadata.append(date__)
                else:
                    date__ = ''
                    metadate.append(date__)

            # update the dictionary of the current email
            EMAIL_DICT.get('{}'.format(CURRENT_EMAIL_NAME)) \
                                            .append(metadata)
            return f
        return wrapper
    return decorator
            
            
                
def open_current_email(func):
    """open the current email for data extraction"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        global EMAIL_COUNTER, CURRENT_EMAIL_NAME, SUBPARTS, CURRENT_EMAIL, EMAIL_DICT
        f = func(*args, **kwargs)
        CURRENT_EMAIL_NAME = f[EMAIL_COUNTER]
        #print("CURRENT_EMAIL: {}".format(f[EMAIL_COUNTER]))
        EMAIL_DICT.update({'{}'.format(f[EMAIL_COUNTER]): []})
        with open(f[EMAIL_COUNTER], 'rb') as email_:
            mm_file = mmap.mmap(email_.fileno(), 0, access=mmap.ACCESS_READ)
            CURRENT_EMAIL = email.message_from_file(email_)
            return email.message_from_file(email_)
    return wrapper
            
        
def get_emails(func):
    """return a list of all email files in current directory"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        f = func(*args, **kwargs)
        g = glob('*.eml')
        #print("Num emails: {}".format(len(g)))
        return g
    return wrapper

@extract_email_contents(write_to_file=False, filename=None)
@get_email_meta_data(to_=True, from_=True, subject_=True, date_=True)
@open_current_email
@get_emails
def email_data_pipeline(fp):
    """main pipeline function on which
    all decorators will apply to"""
    try:
        if os.path.exists(fp):
            os.chdir(fp)
        else:
            raise FileExistsError("Error: path not found")
    except FileExistsError as e:
        return e
    finally:
        print("Current diretory: {}".format(os.getcwd()))   
    
        
def main(num_files_to_read):
    global EMAIL_DICT, EMAIL_COUNTER
    p = "Z:\\WinRisk\\P&C Business Analytics\\Claims Narratives"
    while EMAIL_COUNTER < num_files_to_read:
        email_pipeline = email_data_pipeline(p)
        
            
    
    

if __name__ == '__main__':
    main(10)
    
