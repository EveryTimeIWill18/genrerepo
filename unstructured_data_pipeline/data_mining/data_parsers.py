"""
data_parsers
~~~~~~~~~~~~~
"""
import os
import sys
import stat
import struct
import string
import shutil
import zipfile
import subprocess
import pytesseract
from PIL import Image
from email import policy
from datetime import datetime
from bs4 import BeautifulSoup
from PyPDF2 import PdfFileReader
from email.parser import BytesParser
from xml.etree.cElementTree import XML
from configparser import ConfigParser
from unstructured_data_pipeline.data_mining.concrete_ocr import PdfOCR
from unstructured_data_pipeline.data_mining.file_encoders import  (SPACES, NEWLINE, TABS,
                RTF_ENCODING, destinations, specialchars, PUNCTUATION, WHITESPACE, BARS)
from unstructured_data_pipeline.logging.logging_config import BaseLogger
from unstructured_data_pipeline.config_path import config_file_path

# Global Variables #
####################################################################################################
d = str(datetime.today())[:10].replace("-","_")

# path to tesseract executable
pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files\\Tesseract-OCR\\tesseract'

# setup logging configuration
config = ConfigParser()
config.read(config_file_path)
sections = config.sections()
logfile = "dms_unst_pipeline_log_" + d
logger = BaseLogger(config.get(sections[2], 'DMS_LOGGING'), logfile)
logger.config() # use the default logging configuration

# Data Parser Classes #
####################################################################################################

class FileGenerator(object):
    """Python Generator Class"""
    def __init__(self, project_file_path: str, file_ext: str):
        self.project_file_path = project_file_path
        self.file_ext = file_ext

    def __iter__(self):
        try:
            if os.path.isdir(self.project_file_path):
                for name in os.scandir(self.project_file_path):
                    if os.path.splitext(os.path.basename(name))[-1] == '.'+self.file_ext:
                        yield os.path.join(self.project_file_path, os.path.basename(name))
        except StopIteration:
            print("Finished processing files")


class EmlParser(object):
    """Eml File Parser"""

    def __init__(self):
        self.mapping_dict: dict = {}
        self.file_counter: int = 0
        self.error_file_counter: int = 0
        self.error_files: list = []
        #logger.info(info=f"Eml File Parser beginning ...")

    def extract_text(self, current_file: str) -> dict:
        try:
            with open(current_file, 'rb') as eml_file:
                #logger.info(info=f'Eml file: {os.path.basename(current_file)}')
                msg = BytesParser(policy=policy.default).parse(eml_file)
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == 'text/html':
                            soup = BeautifulSoup(part.get_content(), 'html.parser')
                            body = soup.findAll(text=True)  # extract the text

                            # check if the body of the eml file is None or 0
                            if not body:
                                self.error_file_counter += 1
                                self.error_files.append(os.path.basename(current_file))
                                return f"No text body in email: {os.path.basename(current_file)}"
                            else:
                                # process the text list into a formatted string
                                body = ' '.join(body) \
                                    .translate(str.maketrans('', '', string.punctuation)) \
                                    .lower()
                                body = SPACES.sub(" ", body)
                                body = NEWLINE.sub("", body)
                                body = TABS.sub(" ", body)
                                body = ''.join([i if ord(i) < 128 else ' ' for i in body])
                                #print(f"body := {body}")
                                # UPDATE: added 6/20/2019
                                if len(body) == 0:
                                    # not text was extracted from this file; add to error files list
                                    self.error_file_counter += 1
                                    self.error_files.append(os.path.basename(current_file))
                                    logger.error(error=f"Eml file: {os.path.basename(current_file)} has no text body.")

                            # update the mapping dict if the file is not currently in the mapping dictionary
                            if os.path.basename(current_file) not in self.mapping_dict.keys():
                                self.mapping_dict[os.path.basename(current_file)] = body
                                self.file_counter += 1
                                return {os.path.basename(current_file): body}
                            else:
                                return f"Eml File: {os.path.basename(current_file)} has already been read in."
                else:
                    # UPDATE: added 6/20/2019
                    # if email is not multipart, we can extract the text directly
                    try:
                        if msg.get_content_type() == 'text/html':
                            soup = BeautifulSoup(msg.get_content(), 'html.parser')
                            body = soup.findAll(text=True)  # extract the text
                            # process the text list into a formatted string
                            body = ' '.join(body) \
                                .translate(str.maketrans('', '', string.punctuation)) \
                                .lower()
                            body = SPACES.sub(" ", body)
                            body = NEWLINE.sub("", body)
                            body = TABS.sub(" ", body)
                            body = ''.join([i if ord(i) < 128 else ' ' for i in body])
                            #print(f"body := {body}")
                            # update the mapping dict if the file is not currently in the mapping dictionary
                            if os.path.basename(current_file) not in self.mapping_dict.keys():
                                self.mapping_dict[os.path.basename(current_file)] = body
                                self.file_counter += 1
                                return {os.path.basename(current_file): body}
                            else:
                                return f"Eml File: {os.path.basename(current_file)} has already been read in."
                    except Exception as e:
                        # NOTE: *added 06/28/2019*
                        self.error_file_counter += 1
                        self.error_files.append(os.path.basename(current_file))
                        logger.error(error=f'Eml file: {os.path.basename(current_file)} could not be text mined.')
                        logger.error(error=e)
        except (OSError, Exception) as e:
            # update the error file information
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))
            logger.error(error=f'Eml file: {os.path.basename(current_file)} could not be text mined.')
            logger.error(error=e)


class RtfParser(object):
    """Rtf File Parser"""
    def __init__(self):
        self.mapping_dict: dict = {}
        self.file_counter: int = 0
        self.error_file_counter: int = 0
        self.error_files: list = []
        #logger.info(info=f"Rtf File Parser beginning ...")

    def extract_text(self, current_file) -> dict:
        """Extract the current rtf file's text"""
        try:
            with open(current_file, 'rb') as f:
                text = f.read().decode('utf-8')
                stack = []
                ignorable = False
                ucskip = 1
                curskip = 0
                out = []  # Output buffer.
                for match in RTF_ENCODING.finditer(text):
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
                result = ''.join(ch for ch in result if ch not in PUNCTUATION)
                result = SPACES.sub(" ", result)
                result = ''.join(result)

                # update self.fileDict
                self.mapping_dict.update({os.path.basename(current_file): result.lower()})
                self.file_counter += 1

                return {os.path.basename(current_file): result.lower()}
        except (OSError, Exception) as e:
            self.file_counter += 1
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            logger.error(error=f'Rtf file: {os.path.basename(current_file)} could not be text mined.')
            logger.error(error=e)

class DocxParser(object):
    """Docx File Parser"""

    def __init__(self):
        self.mapping_dict: dict = {}
        self.file_counter: int = 0
        self.error_file_counter: int = 0
        self.error_files: list = []
        #logger.info(info=f"Docx File Parser beginning ...")

    def extract_text(self, current_file):
        """Extract the current docx file's text"""
        try:
            WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
            PARA = WORD_NAMESPACE + 'p'
            TEXT = WORD_NAMESPACE + 't'

            # unzip the current document and read its contents
            if os.path.getsize(current_file) > 0:
                document = zipfile.ZipFile(current_file)
                for i, _ in enumerate(document.infolist()):
                    if document.infolist()[i].filename == 'word/document.xml':
                        xml_content = document.read('word/document.xml')
                        document.close()
                        tree = XML(xml_content)  # parse the xml document
                        paragraphs = []
                        for paragraph in tree.getiterator(PARA):
                            texts = [
                                node.text
                                for node in paragraph.getiterator(TEXT)
                                if node.text
                            ]
                            if texts:
                                # process the text list into a formatted string
                                texts = ' '.join(texts) \
                                    .translate(str.maketrans('', '', string.punctuation)) \
                                    .lower()
                                texts = SPACES.sub(" ", texts)
                                texts = texts[6:]
                                texts = BARS.sub("", texts)
                                texts = NEWLINE.sub(" ", texts)
                                texts = TABS.sub(" ", texts)
                                # attempt to remove special characters
                                texts = ''.join([i if ord(i) < 128 else ' ' for i in texts])
                                paragraphs.append(texts)

                        # update the mapping file
                        self.mapping_dict.update({os.path.basename(current_file): ''.join(paragraphs)})
                        # increment the file counter
                        self.file_counter += 1
                        return {os.path.basename(current_file): ''.join(paragraphs)}
                else:
                    self.error_file_counter += 1
                    self.error_files.append(os.path.basename(current_file))  # added: 6/28/2019
                    logger.error(error=f"Docx file: {os.path.basename(current_file)} is corrupt.") # added: 6/28/2019
            else:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
                logger.error(error=f"Current docx file: {current_file}\nFile size: {os.path.getsize(current_file)}")

        except (OSError, Exception) as e:
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))  # added: 4/16/2019
            logger.error(error=f'Docx file: {os.path.basename(current_file)} could not be text mined.')
            logger.error(error=e)


class DocParser(object):
    """Doc File Parser"""

    def __init__(self, r_executable: str, r_path: str, r_script: str):
        self.mapping_dict: dict = {}
        self.file_counter: int = 0
        self.error_file_counter: int = 0
        self.error_files: list = []
        self.r_executable = r_executable
        self.r_path = r_path
        self.r_script = r_script
        #logger.info(info=f"Doc File Parser beginning ...")

    def run_doc_to_csv_r_script(self, file_path: str, timeout):
        """run the R script that converts .doc to .csv"""
        filepath = '"' + file_path + '"'        # path to the raw data
        writepath = os.path.join(os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\doc_to_csv')
        time_out = '"' + timeout + '"'
        try:
            # run the R script
            subprocess.call([self.r_executable, os.path.join(
                self.r_path, self.r_script), filepath, writepath, time_out], shell=True)
        except Exception as e:
            logger.error(error=f'An error occurred while trying to run R script')
            logger.error(error=e)

    def remove_temp_doc_to_csv_files(self, doc_to_csv_write_path: str):
        """remove the raw converted doc to csv files from the specified directory"""
        try:
            # recursively remove files from the directory if it contains files
            if len(os.listdir(doc_to_csv_write_path)) > 0:
                rm_attempts = 0
                while rm_attempts < 5:
                    # check that you have delete access
                    if os.access(doc_to_csv_write_path, os.W_OK) and os.access(doc_to_csv_write_path, stat.S_IWGRP):
                        # delete the files when finished processing
                        for f in os.listdir(doc_to_csv_write_path):
                            current_file = os.path.join(doc_to_csv_write_path, f)
                            try:
                                if os.path.isfile(current_file):
                                    os.unlink(current_file)
                            except Exception as e:
                                logger.error(error=f'An error occurred while trying to delete file: {f}')
                                logger.error(error=e)
                        break
                    else:
                        # if write access is False
                        os.chmod(doc_to_csv_write_path, stat.S_IRWXU| stat.S_IRWXG| stat.S_IRWXO)
                        rm_attempts += 1
                else:
                    # number of remove attempts exceeded
                    dir_name = os.path.splitext(os.path.split(doc_to_csv_write_path)[1])[0]
                    print(f"WARNING: Doc to Csv Directory: {dir_name} could not be deleted!")
            else:
                # the directory does not contain any files
                dir_name = os.path.splitext(os.path.split(doc_to_csv_write_path)[1])[0]
                print(f"Doc to Csv Directory: {dir_name} is empty.")
        except (OSError, Exception) as e:
            logger.error(error=f'An error occurred when trying to remove temporary converted Csv[doc] files.')
            logger.error(error=e)

    def extract_text(self, current_file: str):
        """extract the contents from the converted .doc files"""
        try:
            with open(current_file, 'r', errors='replace', encoding='utf-8') as f:
                text = f.read()
                text = SPACES.sub(" ", text)
                text = text[6:]
                text = BARS.sub("", text)
                text = NEWLINE.sub(" ", text)
                text = TABS.sub(" ", text)
                text = text.translate(str.maketrans('', '', string.punctuation)).lower()

                # attempt to remove special characters
                text = ''.join([i if ord(i) < 128 else ' ' for i in text])
                text = str(text).encode('ascii', errors='ignore').decode('utf8')

                # update the mapping dict
                self.mapping_dict.update({os.path.basename(current_file): text})
                self.file_counter += 1
                return {os.path.basename(current_file): text}
        except (OSError, Exception) as e:
            if current_file not in self.error_files:
                self.error_file_counter += 1
                self.error_files.append(os.path.basename(current_file))
            logger.error(error=f'Rtf file: {os.path.basename(current_file)} could not be text mined.')
            logger.error(error=e)

class PdfParser(object):

    def __init__(self):
        self.mapping_container: list[dict] = []
        self.file_counter: int = 0
        self.error_file_counter: int = 0
        self.error_files: list = []

        self.current_pdf: PdfFileReader = None  # the current pdf
        self.current_file: str = None    # the name of the current file
        self.mapping_pointer: int = -1   # pointer to the current pdf

        self.pdf_ocr = PdfOCR()     # create an instance of the PdfOCR class
        self.opened_file = None     # stores the currently opened pdf
        self.CCITT_group = 4        # used to properly extract .tiff images

        #logger.info(info=f"Pdf File Parser beginning ...")

    def insert_new_pdf(self, current_file: str):
        """insert the new pdf file along with the number of pages"""

        self.current_file = current_file    # set the current file
        self.mapping_pointer += 1           # set pointer to correct file position
        try:
            self.opened_file = open(current_file, 'rb')
            self.current_pdf = PdfFileReader(self.opened_file)
            num_pages = self.current_pdf.numPages
            print(f"Current pdf: {os.path.basename(current_file)}")

            # create a temporary dictionary
            temp = {}
            temp['file_name'] = os.path.basename(current_file)
            # iterate through the page numbers to update the mapping
            for i in range(num_pages):
                temp[i] = ''

            # update the mapping container
            self.mapping_container.append(temp)
        except Exception as e:
            logger.error(error=e)

    def tiff_header_CCITT(self, width, height, img_size, CCITT_group=4):
        """
        Creates a header for extracting images from .tiff pdf image file format.
        """
        tiff_header_struct = '<' + '2s' + 'h' + 'l' + 'h' + 'hhll' * 8 + 'h'
        self.CCITT_group = CCITT_group

        return struct.pack(tiff_header_struct, b'II',  # Byte order indication: Little indian
                           42,  # Version number (always 42)
                           8,  # Offset to first IFD
                           8,  # Number of tags in IFD
                           256, 4, 1, width,  # ImageWidth, LONG, 1, width
                           257, 4, 1, height,  # ImageLength, LONG, 1, length
                           258, 3, 1, 1,  # BitsPerSample, SHORT, 1, 1
                           259, 3, 1, self.CCITT_group,  # Compression, SHORT, 1, 4 = CCITT Group 4 fax encoding
                           262, 3, 1, 0,  # Threshholding, SHORT, 1, 0 = WhiteIsZero
                           273, 4, 1, struct.calcsize(tiff_header_struct),  # StripOffsets, LONG, 1, len of header
                           278, 4, 1, height,  # RowsPerStrip, LONG, 1, length
                           279, 4, 1, img_size,  # StripByteCounts, LONG, 1, size of image
                           0  # last IFD
                           )

    def ocr_current_pdf(self, full_file_name: str, write_path: str, current_page):
        """extract images from the current pdf"""
        try:
            # get the xObject
            xObject = current_page['/Resources']['/XObject'].getObject()
            # sub page counter
            m = 0
            for obj in xObject:
                # if current object is an image
                if xObject[obj]['/Subtype'] == '/Image':
                    # get the image size and the raw image data(bytes)
                    size = (xObject[obj]['/Width'], xObject[obj]['/Height'])
                    data = xObject[obj]._data
                    if xObject[obj]['/ColorSpace'] == '/DeviceRGB':
                        mode = "RGB"
                    else:
                        mode = "P"

                    if xObject[obj]['/Filter'] == ['/JBIG2Decode']:
                        print("['/JBIG2Decode'] image type is not supported")

                    # image file is .tiff
                    if xObject[obj]['/Filter'] == '/CCITTFaxDecode':
                        pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                        if not os.path.exists(os.path.join(write_path, pdf_name)):
                            # create a directory for the current pdf
                            new_dir = os.path.join(write_path, pdf_name)
                            os.mkdir(new_dir)

                        # set the correct value of the struct
                        if xObject[obj]['/DecodeParms']['/K'] == -1:
                            self.CCITT_group = 4
                        else:
                            self.CCITT_group = 3

                        # get the image file data
                        width = xObject[obj]['/Width']
                        height = xObject[obj]['/Height']
                        data = xObject[obj]._data
                        img_size = len(data)
                        # use the tff header method to correctly extract the image file
                        tiff_header = self.tiff_header_CCITT(
                            width=width, height=height, img_size=img_size, CCITT_group=self.CCITT_group
                        )
                        # save the image file
                        img_name = f'ImgFilePage{current_page}_{m}.tiff'
                        with open(os.path.join(new_dir, img_name), 'wb') as img_file:
                            img_file.write(tiff_header + data)
                        m += 1  # increment the image per page counter

                    # image file is .png
                    elif xObject[obj]['/Filter'] == '/FlateDecode':
                        # create a directory for the image
                        pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                        if not os.path.exists(os.path.join(write_path, pdf_name)):
                            # create a directory for the current pdf
                            new_dir = os.path.join(write_path, pdf_name)
                            os.mkdir(new_dir)

                        # save the image file
                        img = Image.frombytes(mode, size, data)
                        img.save(os.path.join(new_dir, f'ImgFilePage{current_page}_{m}.png'))
                        m += 1  # increment the image per page counter

                    # image file is .jpg
                    elif xObject[obj]['/Filter'] == '/DCTDecode':
                        # create a directory for the image
                        pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                        if not os.path.exists(os.path.join(write_path, pdf_name)):
                            # create a directory for the current pdf
                            new_dir = os.path.join(write_path, pdf_name)
                            os.mkdir(new_dir)  # increment the image per page counter

                            # save the image file
                            img = open(os.path.join(new_dir, f'ImgFilePage{current_page}_{m}.jpg'), "wb")
                            img.write(data)
                            img.close()
                            m += 1

                    # image file is .jp2
                    elif xObject[obj]['/Filter'] == '/JPXDecode':
                        # create a directory for the image
                        pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                        if not os.path.exists(os.path.join(write_path, pdf_name)):
                            # create a directory for the current pdf
                            new_dir = os.path.join(write_path, pdf_name)
                            os.mkdir(new_dir)  # increment the image per page counter

                        # save the image file
                        img = open(os.path.join(new_dir, f'ImgFilePage{current_page}_{m}.jpg'), "wb")
                        img.write(data)
                        img.close()
                        m += 1
                    else:
                        print(f"Pdf: {os.path.basename(full_file_name)} has no images on page: {current_page}")
        except Exception as e:
            logger.error(error=f'An error occurred trying to extract images from pdf: {os.path.basename(full_file_name)} could not be text mined.')
            logger.error(error=e)

    def extract_text(self, current_file):
        try:
            # add the new file and load in the
            self.insert_new_pdf(current_file=current_file)
            pg = 0

            while pg < self.current_pdf.numPages:
                try:
                    current_page = self.current_pdf.getPage(pg)
                    # extract the contents of the pdf
                    text = list(str(current_page.extractText()).splitlines())
                    text = ''.join(text)

                    # if the length of the text is > 0, clean and update the mapping container
                    if len(text) > 0:
                        text = SPACES.sub(" ", text)
                        text = NEWLINE.sub(" ", text)
                        text = TABS.sub(" ", text)
                        text = text.translate(str.maketrans(' ', ' ', string.punctuation)).lower()
                        text = ''.join([i if ord(i) < 128 else ' ' for i in text])

                        # load the data into the mapping data structure
                        #self.mapping_container[self.mapping_pointer].update({pg: text})
                        self.mapping_container[self.mapping_pointer][pg] = text

                        # # extract the images from the current pdf if any images exist
                        # write_path = os.path.join(
                        #     os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\pdf_img'
                        # )
                        # self.ocr_current_pdf(
                        #     full_file_name=current_file,
                        #     write_path=write_path,
                        #     current_page=current_page
                        # )
                    else:
                        # NOTE: *added (06/28/2019)*
                        logger.error(error=f'Pdf file: {os.path.basename(current_file)} has no raw text to mine.')
                    pg += 1 # increment the page counter
                except Exception as e:
                    self.error_file_counter += 1
                    self.error_files.append(os.path.basename(current_file))
                    logger.error(error=f'Pdf file: {os.path.basename(current_file)} could not be text mined.')
                    logger.error(error=e)

            # close the currently opened pdf
            self.opened_file.close()

            # # extract images from the current pdf
            # self.pdf_ocr.extract_pdf_image(
            #     full_file_name=current_file,
            #     write_path=os.path.join(os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\pdf_img')
            # )
        except Exception as e:
            self.error_file_counter += 1
            self.error_files.append(os.path.basename(current_file))
            logger.error(error=f'An error has occurred in PdfParser.extract_text')
            logger.error(error=e)