"""
pdf_ocr_script_two
~~~~~~~~~~~~~~~~~~
"""

import io
import os
import re
import time
import stat
import shutil
import struct
import string
import PyPDF2
import minecart
import pytesseract
from PIL import Image
from pprint import pprint
from datetime import datetime
from wand.image import Image as wand_img
from unstructured_data_pipeline.data_mining.file_encoders import *



class PdfOcrTwo(object):
    """A second version of the pdf Ocr script"""

    def __init__(self):
        # path to write extracted pdf image files
        self.write_path = os.path.join(
                os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\pdf_img'
            )

        # path to write ocr'ed text
        self.ocr_write_path = os.path.join(
            os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\pdf_text'
        )

    def extract_img_minecart(self, full_file_name: str):
        """extract pdf images using minecart"""
        try:
            pdf_doc = open(full_file_name, 'rb')    # open the current pdf
            doc = minecart.Document(pdf_doc)
            for page in doc.iter_pages():
                m = 0   # counter for the number of images on the current page of the current pdf
                for i in range(len(page.images)):
                    try:
                        im = page.images[i].as_pil()    # convert the image into a PIL image
                        name = os.path.join(
                            self.write_path, f'{os.path.basename(full_file_name)}_{i}_{m}.jpg'
                        )
                        m += 1
                        im.save(name)
                    except Exception as e:
                        print(e)
            pdf_doc.close() # close the current pdf
        except Exception as e:
            print(e)

    def ocr_image_file(self, current_img_file: str):
        """ocr the extracted pdf image files"""
        try:
            if os.path.isdir(self.write_path):
                img_ = wand_img(filename=os.path.join(self.write_path, current_img_file), resolution=300)
                wnd_img = wand_img(image=img_).make_blob(
                    format='jpeg'
                )
                print("In ocr_image_file")
                im = Image.open(io.BytesIO(wnd_img))
                # extract and clean the text
                text = pytesseract.image_to_string(im, lang='eng')
                text = ''.join(text) \
                    .translate(str.maketrans('', '', string.punctuation)) \
                    .lower()
                text = SPACES.sub(" ", text)
                # text = NEWLINE.sub("", text)
                # text = TABS.sub("", text)
                # text = ''.join([i if ord(i) < 128 else ' ' for i in text])
                return  text

        except Exception as e:
            print(e)
            print(f"An error has occurred while trying to ocr img file: {current_img_file}")