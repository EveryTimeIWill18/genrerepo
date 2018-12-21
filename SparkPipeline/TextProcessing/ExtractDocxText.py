import os
import shutil
import glob
import zipfile
import threading
import pickle
import time
import pandas as pd
from datetime import datetime
from xml.etree.cElementTree import XML

pd.options.display.max_columns = 100

def extract_docx_content():
    """
    Extract text content from .docx file
    :return: pandas DataFrame
    """

    # --- Namespace information needed to extract content
    WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
    PARA = WORD_NAMESPACE + 'p'
    TEXT = WORD_NAMESPACE + 't'

    # --- Store fully processed docx files
    processed_docx_files = dict()

    pathInput = 'W:\\WinRisk\\PC_BusinessAnalytics\\SA_Claims\\SA_2'
    os.chdir(pathInput)
    docx_files = glob.glob('*.docx')
    # --- Extract text from docx extension
    for docx in docx_files:
        try:
            document = zipfile.ZipFile(docx)
            xml_content = document.read('word/document.xml')
            document.close()
            tree = XML(xml_content)

            #print("docx file {} being processed".format(docx))
            # --- load data into paragraphs list
            paragraphs = []
            for paragraph in tree.getiterator(PARA):
                texts = [node.text
                        for node in paragraph.getiterator(TEXT)
                        if node.text]
                # --- texts into a single text string
                if texts:
                    paragraphs.append(' '.join(texts).lower())
            processed_docx_files.update({docx: ''.join(paragraphs)})
        except:
            print("An error occured trying to parse {}".format(docx))

    # --- Convert python dictionary to a dataframe
    return pd.DataFrame.from_dict(processed_docx_files, orient='index')
