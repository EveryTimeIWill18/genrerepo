"""
WordDocEncodings
----------------
This module stores the different encodings that are found
in the extracted text of a Microsoft Word document.
"""
import os
import re
import string

from pprint import pprint


# - Encodings to be removed
X07 = '\x07'
X0C = '\x0c'
X15 = '\x15'
X07RN = "'\\x07\\r'\n"
NEWLINE = '\\n'
RETURN = '\r'
TAB ='\t'
BACKSPACE = '\b'
SINGLEQUOTE = "'"
DOUBLEQUOTE = '"'

CHARACTER_ENCODINGS = re.compile(r"(\n|\r|\\t|\t|\b|\\r|\\x07|\x07|\x15|\\x15|"
                                 r"\x0c|\\x0c|\\x01|\x01|\xa0|\\xa0|\x0b|\\x0b)")

DICTIONARY_KEYS = re.compile(r"(\d{6,15}\s-\sClaim Narrative.doc:\s)")
DICTIONARY_KEY_IDS = re.compile(r"(\d{6,15}\s-\sClaim Narrative.doc)")


file_path = "N:\\USD\\Business Data and Analytics\\Reference\\Python\\WebScraping"

files = list(filter(lambda x: str(x).endswith('.txt'),
                    list(map(lambda y: os.path.join(file_path, y),
                             os.listdir(file_path)))))




file_contents = open(files[0], 'r').read()
cleaned_contents = re.sub(r"'", "", file_contents)
cleaned_contents = re.sub(r"(\n|\r|\\t|\t|\b|\\r|\\x07|\x07|\x15|\\x15|\x0c|\\x0c|\\x01|\x01|\xa0|\\xa0|\x0b|\\x0b)", "", cleaned_contents)
cleaned_contents = re.sub(r"(\s+|\\s+)", " ", cleaned_contents)


#break_points = [m.start() for m in patternOne.finditer(file_contents)]
keysStart = [m.start() for m in DICTIONARY_KEYS.finditer(cleaned_contents)]

#pprint(cleaned_contents[1:2358])
#pprint(DICTIONARY_KEYS.findall(cleaned_contents))
pprint(DICTIONARY_KEY_IDS.findall(cleaned_contents))
#pprint(cleaned_contents[0:35])
