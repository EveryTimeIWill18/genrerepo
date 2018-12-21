# -*- coding: utf-8 -*-
import os
import email
import email.iterators
import glob
import re
import string
import mmap
import numpy as np
import pandas as pd
from functools import wraps, reduce, partial
from pprint import pprint
import codecs

ERROR_FILES = []

test_rtf = "Z:\\WinRisk\\P&C Business Analytics\\Claims Narratives\\Fw- Garrie v Summitt  GRC#4162414-  #959533-186.rtf"
f = "Z:\\WinRisk\\PC_BusinessAnalytics\\Claims Narratives"

def set_file_type(f_type, f_path):
    """set the file to to search"""
    if os.path.exists(f_path):
        os.chdir(f_path)
    return np.array(list(glob.glob('*.{}'.format(f_type))))


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

        # store eml contents. convert to numpy array
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
    text = unicode(text.read(), errors='ignore').encode('utf-8')
    stack = []
    ignorable = False
    ucskip = 1
    curskip = 0
    out = []  # Output buffer.
    for match in pattern.finditer(text):
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
    return result.lower()


def load_data(data, chunk_size):
    """load data into pandas series"""
    global ERROR_FILES
    data_dict = dict()
    counter = 0
    while counter < chunk_size:
        try:
            d = data[counter]
            f_type = str(d).split(".")[-1]
            if f_type == 'eml':
                print("extension: eml")
                e = extract_eml_data(d)
                pprint(e)
                data_dict.update({d: np.array(e)})
            if f_type == 'rtf':
                print("extension: rtf")
                r = extract_rtf_data(d)
                #pprint(r)
                data_dict.update({d: np.array(r)})
            counter += 1
        except:
            print("Error in decoding text")
            print("adding {} to ERROR_FILES".format(d))
            ERROR_FILES.append(d)
            counter += 1

    s = to_series(data_dict)
    return s

def to_series(a_dict):
    """convert dictionary to data frame"""
    s = pd.Series(data=a_dict, index=np.array(a_dict.keys()), name='File Names')
    return s
