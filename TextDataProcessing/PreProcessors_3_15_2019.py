# -*- coding: utf-8 -*-
"""
pre_processors
~~~~~~~~~~~~~~
This module cleans and extracts text from various document types.
So far, the module is able to handle .eml, .rtf, .docx, .doc, and .pdf files.
In the future, it will be able to handle .html, .xml and other file types.

"""

import os
import io
import csv
import chardet
from scandir import scandir
import pickle
import email
import urllib
import email.iterators
import glob
import re
import string
import mmap
import numpy as np
import pandas as pd
from functools import wraps, reduce, partial
from datetime import datetime
from pprint import pprint
import codecs
from collections import OrderedDict, Container
from sqlalchemy import (create_engine, MetaData,
                        Table, Column, select, or_, and_, func, INT)

# - Constants
claims_narrative = "Z:\\WinRisk\\PC_BusinessAnalytics\\Personal_Umbrella\\Claims_Narrative_Docs"

DATA_CONTAINER = {}
RTF_CONTAINER  = {}
EML_CONTAINER  = {}
PDF_CONTAINER  = {}
DOC_CONTAINER  = {}
DOCX_CONTAINER = {}

class ExtractedContent(object):
    """
    ExtractedContent
    ~~~~~~~~~~~~~~~~
    Container for the extracted contents
    from the various file types.
    """
    DATA_CONTAINER = {}

    @classmethod
    def load_content(cls, fileExt, data):
        """load extracted contents"""
        if fileExt == 'rtf':
            pass
        if fileExt == 'eml':
            pass
        if fileExt == 'doc':
            pass
        if fileExt == 'docx':
            pass



def load_data(filePath, fileExt):
    """Load in the data using a generator."""
    try:
        if os.path.isdir(filePath):
            for entry in scandir(filePath):
                if os.path.splitext(entry.name)[-1] == '.' + fileExt:
                    yield os.path.join(filePath, entry.name)
    except OSError:
        print("An OSError has occurred")


def load_sql_data():
    """Load sql data into python"""
    # --- sql connection setup
    DRIVER = '{SQL Server}'
    base_connection = "driver={};" \
                      "Server=USTRNTPD334;" \
                      "Database=PCNA_Claims;" \
                      "Trusted_Connection=yes".format(DRIVER)
    parser = urllib.quote_plus(base_connection)
    # - create an engine
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % parser)
    # - connect to data
    connection = engine.connect()
    # - load metadata
    metadata = MetaData()
    # - load the correct table
    personal_umbrella = Table(
        'Personal_Umbrella_DMS_Mapping_4', metadata,
        autoload=True, autoload_with=engine
    )

    # - setup queries for all file extension types
    selector = map(
        lambda x: select([personal_umbrella.c.claim_id, personal_umbrella.c.files]) \
                        .where(personal_umbrella.c.formats == '{}'.format(x)),
                            [u'eml', u'rtf', u'msw8', u'pdf', u'html', u'docx']
    )
    # - build data frames
    create_dfs = map(
        lambda x, y: pd.DataFrame([i for i in connection.execute(x)],
                                               columns=['Claim_Id', y]),
                     selector, [u'Eml Files', u'Rtf Files', u'Msw8 Files',
                                u'Pdf Files', u'Html Files', u'Docx Files']
    )
    return create_dfs



def to_pickle(source, pkl_file, data):
    """Write data structure to pickle file"""
    try:
       if os.path.exists(source):
            os.chdir(source)
            current_date = str(datetime.today())[:10]
            save_ = pkl_file + '_' + current_date + ".pickle"
            try:
                to_pkl = open(save_, 'wb')
                pickle.dump(data, to_pkl)
                to_pkl.close()
            except pickle.PicklingError:
                PICKLE_STATUS = -1
                print("PickleError: Could not pickle object")
                print("PICKLE_STATUS: ", PICKLE_STATUS)

       else:
            raise OSError("OSError: Path not found.")
    except OSError as e:
        print(e)


def from_pickle(source, pkl_file):
    """Open specified pickle file"""
    try:
        if os.path.exists(source):
            os.chdir(source)
            try:
                from_pkl = open(pkl_file, 'rb')
                load_pkl = pickle.load(from_pkl)
                from_pkl.close()
                return load_pkl
            except pickle.PicklingError:
                PICKLE_STATUS = -1
                print("PicklingError: An error occured in reading pkl file.")
                print("PICKLE_STATUS", PICKLE_STATUS)
        else:
            raise OSError("OSError: Path {}, could not be found".format(source))
    except OSError as e:
        print(e)


def extract_eml_data(data):
    """extract eml data"""

    tags = re.compile(r'\<.*?\>')
    spaces = re.compile(r'\s+')
    with open(data, 'rb') as msg:
        # get the eml data
        e = email.message_from_file(msg)

        # create an iterator
        email_contents = email \
                        .iterators \
                        .body_line_iterator(e, decode=True)

        # - store eml contents. convert to numpy array
        extracted_contents = []
        for c in email_contents:
            extracted_contents.append(c)

        # extract email message body
        raw_data = ''.join(extracted_contents)
        front = raw_data.find('<html>')
        back = raw_data.find('</html>')
        subset_raw_data = raw_data[front:back]

        # - clean content with regex
        subset_raw_data = tags.sub('<>', subset_raw_data)
        parsed_result = ''.join(subset_raw_data)
        parsed_result = parsed_result.translate(
            string.maketrans(" ", " "),
            string.punctuation
        ).lower()
        parsed_result = spaces.sub(' ', parsed_result)

        # - final output
        final_result = ''.join(parsed_result)
        return final_result


def extract_rtf_data(text):
    """extract the rtf data"""

    pattern = re.compile(r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-f]{2})|\\([^a-z])|([{}])|[\r\n]+|(.)", re.I)
    exclude = set(string.punctuation)
    whitespace = set(string.whitespace)
    large_spaces = re.compile(r'\s+')

    # control words which specify a "destionation".
    destinations = frozenset((
        'aftncn', 'aftnsep', 'aftnsepc', 'annotation', 'atnauthor', 'atndate', 'atnicn', 'atnid',
        'atnparent', 'atnref', 'atntime', 'atrfend', 'atrfstart', 'author', 'background',
        'bkmkend', 'bkmkstart', 'blipuid', 'buptim', 'category', 'colorschememapping',
        'colortbl', 'comment', 'company', 'creatim', 'datafield', 'datastore', 'defchp', 'defpap',
        'do', 'doccomm', 'docvar', 'dptxbxtext', 'ebcend', 'ebcstart', 'factoidname', 'falt',
        'fchars', 'ffdeftext', 'ffentrymcr', 'ffexitmcr', 'ffformat', 'ffhelptext', 'ffl',
        'ffname', 'ffstattext', 'field', 'file', 'filetbl', 'fldinst', 'fldrslt', 'fldtype',
        'fname', 'fontemb', 'fontfile', 'fonttbl', 'footer', 'footerf', 'footerl', 'footerr',
        'footnote', 'formfield', 'ftncn', 'ftnsep', 'ftnsepc', 'g', 'generator', 'gridtbl',
        'header', 'headerf', 'headerl', 'headerr', 'hl', 'hlfr', 'hlinkbase', 'hlloc', 'hlsrc',
        'hsv', 'htmltag', 'info', 'keycode', 'keywords', 'latentstyles', 'lchars', 'levelnumbers',
        'leveltext', 'lfolevel', 'linkval', 'list', 'listlevel', 'listname', 'listoverride',
        'listoverridetable', 'listpicture', 'liststylename', 'listtable', 'listtext',
        'lsdlockedexcept', 'macc', 'maccPr', 'mailmerge', 'maln', 'malnScr', 'manager', 'margPr',
        'mbar', 'mbarPr', 'mbaseJc', 'mbegChr', 'mborderBox', 'mborderBoxPr', 'mbox', 'mboxPr',
        'mchr', 'mcount', 'mctrlPr', 'md', 'mdeg', 'mdegHide', 'mden', 'mdiff', 'mdPr', 'me',
        'mendChr', 'meqArr', 'meqArrPr', 'mf', 'mfName', 'mfPr', 'mfunc', 'mfuncPr', 'mgroupChr',
        'mgroupChrPr', 'mgrow', 'mhideBot', 'mhideLeft', 'mhideRight', 'mhideTop', 'mhtmltag',
        'mlim', 'mlimloc', 'mlimlow', 'mlimlowPr', 'mlimupp', 'mlimuppPr', 'mm', 'mmaddfieldname',
        'mmath', 'mmathPict', 'mmathPr', 'mmaxdist', 'mmc', 'mmcJc', 'mmconnectstr',
        'mmconnectstrdata', 'mmcPr', 'mmcs', 'mmdatasource', 'mmheadersource', 'mmmailsubject',
        'mmodso', 'mmodsofilter', 'mmodsofldmpdata', 'mmodsomappedname', 'mmodsoname',
        'mmodsorecipdata', 'mmodsosort', 'mmodsosrc', 'mmodsotable', 'mmodsoudl',
        'mmodsoudldata', 'mmodsouniquetag', 'mmPr', 'mmquery', 'mmr', 'mnary', 'mnaryPr',
        'mnoBreak', 'mnum', 'mobjDist', 'moMath', 'moMathPara', 'moMathParaPr', 'mopEmu',
        'mphant', 'mphantPr', 'mplcHide', 'mpos', 'mr', 'mrad', 'mradPr', 'mrPr', 'msepChr',
        'mshow', 'mshp', 'msPre', 'msPrePr', 'msSub', 'msSubPr', 'msSubSup', 'msSubSupPr', 'msSup',
        'msSupPr', 'mstrikeBLTR', 'mstrikeH', 'mstrikeTLBR', 'mstrikeV', 'msub', 'msubHide',
        'msup', 'msupHide', 'mtransp', 'mtype', 'mvertJc', 'mvfmf', 'mvfml', 'mvtof', 'mvtol',
        'mzeroAsc', 'mzeroDesc', 'mzeroWid', 'nesttableprops', 'nextfile', 'nonesttables',
        'objalias', 'objclass', 'objdata', 'object', 'objname', 'objsect', 'objtime', 'oldcprops',
        'oldpprops', 'oldsprops', 'oldtprops', 'oleclsid', 'operator', 'panose', 'password',
        'passwordhash', 'pgp', 'pgptbl', 'picprop', 'pict', 'pn', 'pnseclvl', 'pntext', 'pntxta',
        'pntxtb', 'printim', 'private', 'propname', 'protend', 'protstart', 'protusertbl', 'pxe',
        'result', 'revtbl', 'revtim', 'rsidtbl', 'rxe', 'shp', 'shpgrp', 'shpinst',
        'shppict', 'shprslt', 'shptxt', 'sn', 'sp', 'staticval', 'stylesheet', 'subject', 'sv',
        'svb', 'tc', 'template', 'themedata', 'title', 'txe', 'ud', 'upr', 'userprops',
        'wgrffmtfilter', 'windowcaption', 'writereservation', 'writereservhash', 'xe', 'xform',
        'xmlattrname', 'xmlattrvalue', 'xmlclose', 'xmlname', 'xmlnstbl',
        'xmlopen',
    ))
    # Translation of some special characters.
    specialchars = {
        'par': '\n',
        'sect': '\n\n',
        'page': '\n\n',
        'line': '\n',
        'tab': '\t',
        'emdash': '\u2014',
        'endash': '\u2013',
        'emspace': '\u2003',
        'enspace': '\u2002',
        'qmspace': '\u2005',
        'bullet': '\u2022',
        'lquote': '\u2018',
        'rquote': '\u2019',
        'ldblquote': '\201C',
        'rdblquote': '\u201D',
    }
    text = codecs.open(text, 'rb')
    text = unicode(text.read(), errors='replace').encode('ISO-8859-1')
    stack = []
    ignorable = False
    ucskip = 1
    curskip = 0
    out = []  # Output buffer.
    for match in pattern.finditer(text):
        result = None
        try:
            word, arg, hex, char, brace, tchar = match.groups()
            if brace:
                curskip = 0
                if brace == '{':
                    # Push state
                    stack.append((ucskip, ignorable))
                elif brace == '}':
                    # Pop state
                    ucskip, ignorable = stack.pop()
            elif char:  # \x (not a letter)
                curskip = 0
                if char == '~':
                    if not ignorable:
                        out.append('\xA0')
                elif char in '{}\\':
                    if not ignorable:
                        out.append(char)
                elif char == '*':
                    ignorable = True
            elif word:  # \foo
                curskip = 0
                if word in destinations:
                    ignorable = True
                elif ignorable:
                    pass
                elif word in specialchars:
                    out.append(specialchars[word])
                elif word == 'uc':
                    ucskip = int(arg)
                elif word == 'u':
                    c = int(arg)
                    if c < 0: c += 0x10000
                    if c > 127:
                        out.append(chr(c))  # NOQA
                    else:
                        out.append(chr(c))
                    curskip = ucskip
            elif hex:  # \'xx
                if curskip > 0:
                    curskip -= 1
                elif not ignorable:
                    c = int(hex, 16)
                    if c > 127:
                        out.append(chr(c))  # NOQA
                    else:
                        out.append(chr(c))
            elif tchar:
                if curskip > 0:
                    curskip -= 1
                elif not ignorable:
                    out.append(tchar)
            result = ''.join(out)
        except:
            print("An error occurred in parsing")
    #result = result.decode('utf-8')

    result = ''.join(ch for ch in result if ch not in exclude)
    #print result
    result = large_spaces.sub(" ", result)
    result = ''.join(result)
    print result.lower()
    final_result = []
    #for ch in result:
    #    if ch  in whitespace:
    #        ch = ' '
    #    else:
    #        pass
    #final_result.append(ch)
    #print final_result
    #final_result = ''.join(final_result)
    return result.lower().decode('ISO-8859-1', errors='replace')




def create_pandas_dataframe(fileExt):
    """Convert dictionary into a pandas DataFrame."""
    global DATA_CONTAINER
    global RTF_CONTAINER
    global EML_CONTAINER

    if fileExt == 'eml':
        try:
            emlDf = pd.DataFrame(EML_CONTAINER.items())
            emlDf.columns = ['filename', 'raw_wordlist']
            pprint(emlDf)
            return emlDf
        except:
            print("An error occurred in creating eml-dict to data frame.")

    if fileExt == 'rtf':
        try:
            rtfDf = pd.DataFrame(RTF_CONTAINER.items())
            rtfDf.columns = ['filename', 'raw_wordlist']
            pprint(rtfDf.head())
            return rtfDf
        except:
            print("An error occurred in creating rtf-dict to data frame.")

    if fileExt == 'doc':
        pass

    if fileExt == 'docx':
        pass


def join_dataframes(fileExt):
    """
    Join the sql and text data frames
    :return:
    """

    if fileExt == 'eml':
        sqlFrame = load_sql_data()[0]
        sqlFrame.columns = ['Claim_Id', 'filename']
        emlFrame = create_pandas_dataframe(fileExt)
        mergedDf = pd.merge(
            left=sqlFrame, right=emlFrame,
            left_on='filename', right_on='filename',
            how='inner'
        )
        mergedDf.set_index('Claim_Id')
        return mergedDf

    if fileExt == 'rtf':
        sqlFrame = load_sql_data()[1]
        sqlFrame.columns = ['Claim_Id', 'filename']
        rtfFrame = create_pandas_dataframe(fileExt)
        mergedDf = pd.merge(
            left=sqlFrame, right=rtfFrame,
            left_on='filename', right_on='filename',
            how='inner'
        )
        mergedDf.set_index('Claim_Id')
        return mergedDf

    if fileExt == 'doc':
        pass

    if fileExt == 'docx':
        pass


def write_frame_to_csv(fileExt, writePath, dataFrame):
    """
    Write the data frame to a csv file.
    :param fileExt:
    :return:
    """
    print("Trying to write to csv ... ")
    if fileExt == 'eml':
        current_date = str(datetime.today())[:10].replace('-', "_")
        writeName = fileExt+'_'+'DataFrame'+'_'+current_date+'.csv'
        if os.path.isdir(writePath):
            print('writing csv ... ')
            finalName = os.path.join(writePath, writeName)
            # - write to csv
            with io.open(finalName, 'w',encoding='utf-8', errors='ignore') as f:
                dataFrame.to_csv(f, header=True, index=False, encoding='utf-8')

    if fileExt == 'rtf':
        current_date = str(datetime.today())[:10].replace('-', "_")
        writeName = fileExt + '_' + 'DataFrame' + '_' + current_date + '.csv'
        if os.path.isdir(writePath):
            print('writing csv ... ')
            finalName = os.path.join(writePath, writeName)
            pprint(finalName)
            # - write to csv
            dataFrame.to_csv(finalName, header=True, index=False, encoding='ISO-8859-1', sep=str(','))

    if fileExt == 'doc':
        pass

    if fileExt == 'docx':
        pass

def write_dict_to_csv(fileExt, writeName):
    """

    :param fileExt:
    :param writeName:
    :return:
    """
    pass

def run_data_processing(filePath, fileExt, chunkSize=1, loadPickle=False, save=False, savePath=None):
    """
    Process the various file types.
    :param data:
    :param chunk_size:
    :return:
    """
    global DATA_CONTAINER
    global RTF_CONTAINER
    global EML_CONTAINER
    dataDict = {}
    dataList = []
    if loadPickle:
        pass
    else:
        if fileExt == 'rtf':
            tempList = ['rtf']
            rawData = load_data(filePath, fileExt)
            counter = 0
            while counter < chunkSize:
                nextData = next(rawData) # create a file generator
                if os.path.basename(nextData) not in RTF_CONTAINER:
                    RTF_CONTAINER[os.path.basename(nextData)] = extract_rtf_data(nextData)
                    dataDict[os.path.basename(nextData)] = extract_rtf_data(nextData)
                    tempList.append(dataDict)
                counter += 1    # increment chunk counter
            dataList.append(tempList)
            #del tempList # remove temp list from memory
            if save:
                assert savePath is not None
                print("Save path = {}".format(savePath))

                # - create output DataFrames
                dataFrame = join_dataframes(fileExt)
                # - write file to csv
                write_frame_to_csv(fileExt, savePath, dataFrame)
            else:
                return create_pandas_dataframe(fileExt)

        if fileExt == 'eml':
            tempList = ['eml']
            rawData = load_data(filePath, fileExt)
            counter = 0
            while counter < chunkSize:
                nextData = next(rawData)  # create a file generator
                if os.path.basename(nextData) not in EML_CONTAINER:
                    EML_CONTAINER[os.path.basename(nextData)] = extract_eml_data(nextData)
                    tempList.append(dataDict)
                counter += 1  # increment chunk counter
            dataList.append(tempList)
            del tempList  # remove temp list from memory

            if save:
                assert savePath is not None
                # - create output DataFrames
                dataFrame = dataFrame = join_dataframes(fileExt)
                # - write file to csv
                write_frame_to_csv(fileExt, savePath, dataFrame)
            else:
                return create_pandas_dataframe(fileExt)
        if fileExt == 'doc':
            pass

        if fileExt == 'docx':
            pass


def main():
    #rtfTextDF = run_data_processing(filePath=claims_narrative, fileExt='rtf', chunkSize=10)
    #emlTextDF = run_data_processing(filePath=claims_narrative, fileExt='eml', chunkSize=10)

    rtfSavePath  = "N:\\USD\\Business Data and Analytics\\Claims_Pipeline_Files\\DataPipelineCsvOutput\\rtf"
    #numRtfFiles  = len([f for f in os.listdir(claims_narrative) if os.path.splitext(f)[-1] == '.rtf'])
    #numEmlFiles  = len([f for f in os.listdir(claims_narrative) if os.path.splitext(f)[-1] == '.eml'])
    #numDocFiles  = len([f for f in os.listdir(claims_narrative) if os.path.splitext(f)[-1] == '.doc'])
    #numDocxFiles = len([f for f in os.listdir(claims_narrative) if os.path.splitext(f)[-1] == '.docx'])

    #rtfGenerator = load_data(filePath=claims_narrative, fileExt='rtf')
    #rtf_doc = extract_rtf_data(rtfGenerator.next())

    # rtf files count: 2176
    rtfTextDf = run_data_processing(
        filePath=claims_narrative, fileExt='rtf',
        chunkSize=21, save=True, savePath=rtfSavePath
    )

if __name__ == '__main__':
    main()
