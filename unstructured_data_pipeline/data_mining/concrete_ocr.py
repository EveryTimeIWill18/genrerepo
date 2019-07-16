"""
concrete_ocr
~~~~~~~~~~~~
"""
import io
import os
import re
import time
import stat
import shutil
import string
import struct
import PyPDF2
import pytesseract
from PIL import Image
from datetime import datetime
from wand.image import Image as wand_img
from unstructured_data_pipeline.data_mining.file_encoders import  (SPACES, NEWLINE, TABS,
                RTF_ENCODING, destinations, specialchars, PUNCTUATION, WHITESPACE, BARS)


# path to tesseract executable
pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files\\Tesseract-OCR\\tesseract'

# Pdf OCR class configuration #
####################################################################################################
class PdfOCR(object):
    """OCR pdf documents"""

    def __init__(self):
        self.CCITT_group = 4
        self.current_pdf_dir: str = None


    def tiff_header_CCITT(self, width, height, img_size, CCITT_group=4):
        """
        Creates a header for extracting images from .tiff pdf image file format.
        """
        tiff_header_struct = '<' + '2s' + 'h' + 'l' + 'h' + 'hhll' * 8 + 'h'
        self.CCITT_group = CCITT_group

        return struct.pack(tiff_header_struct,  b'II',  # Byte order indication: Little indian
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

    def extract_pdf_image(self, full_file_name: str, write_path: str):
        """extract image files from the current pdf"""
        try:
            if os.path.isfile(full_file_name):
                # open the current pdf
                pdf_reader = PyPDF2.PdfFileReader(
                    open(full_file_name, 'rb')
                )
                # get the number of pages
                num_pages = pdf_reader.getNumPages()

                # iterate through each page and extract the pdf's contents
                n = 0
                while n < num_pages:
                    try:
                        # get the current page
                        page = pdf_reader.getPage(n)

                        # get the xObject
                        xObject = page['/Resources']['/XObject'].getObject()

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
                                        self.current_pdf_dir = new_dir
                                        os.mkdir(new_dir)

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
                                    img_name = f'ImgFilePage{n}_{m}.tiff'
                                    with open(os.path.join(self.current_pdf_dir, img_name), 'wb') as img_file:
                                        img_file.write(tiff_header + data)
                                    m += 1  # increment the image per page counter

                                # image file is .png
                                elif xObject[obj]['/Filter'] == '/FlateDecode':
                                    # create a directory for the image
                                    pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                                    if not os.path.exists(os.path.join(write_path, pdf_name)):
                                        # create a directory for the current pdf
                                        new_dir = os.path.join(write_path, pdf_name)
                                        self.current_pdf_dir = new_dir
                                        os.mkdir(self.current_pdf_dir )

                                    # save the image file
                                    img = Image.frombytes(mode, size, data)
                                    img.save(os.path.join(self.current_pdf_dir , f'ImgFilePage{n}_{m}.png'))
                                    m += 1  # increment the image per page counter

                                # image file is .jpg
                                elif xObject[obj]['/Filter'] == '/DCTDecode':
                                    # create a directory for the image
                                    pdf_name = os.path.basename(os.path.splitext(full_file_name)[0])  # current pdf
                                    if not os.path.exists(os.path.join(write_path, pdf_name)):
                                        # create a directory for the current pdf
                                        new_dir = os.path.join(write_path, pdf_name)
                                        self.current_pdf_dir = new_dir
                                        os.mkdir(new_dir)   # increment the image per page counter

                                    # save the image file
                                    img = open(os.path.join(self.current_pdf_dir, f'ImgFilePage{n}_{m}.jpg'), "wb")
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
                                        self.current_pdf_dir = new_dir
                                        os.mkdir(new_dir)  # increment the image per page counter

                                    # save the image file
                                    img = open(os.path.join(self.current_pdf_dir, f'ImgFilePage{n}_{m}.jpg'), "wb")
                                    img.write(data)
                                    img.close()
                                    m += 1

                                else:
                                    print(f"Pdf: {os.path.basename(full_file_name)} has no images on page: {n}")
                    except Exception as e:
                        print(e)
                        print(f"An error has occurred on page: {n} of Pdf: {full_file_name}")
                        n += 1
        except (OSError, Exception) as e:
            print(e)
            print(f"OSError: Could not open pdf file, {full_file_name}")