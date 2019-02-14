import os
import pytesseract
import numpy as np
import pandas as pd
import re
import string
from io import StringIO
import cv2
from datetime import datetime
from pprint import pprint
from PyPDF2 import PdfFileReader, PdfFileWriter
from PIL import Image




SPACES = re.compile(r"(\s+|\\s+)")
PUNCTUATION = re.compile(r"(˘|ˇ|ˆ|˙|˝|\"|'|''|,|˜|˛)")

# - path to data
file_path = "Z:\\"
files = os.listdir(file_path)

# - get the file extensions
file_extensions = [os.path.splitext(f)[1] for f in files]
extensions = set(file_extensions)
extensions.remove('')


class ExtractPdfContent:
    """
    ExtractPdfContent
    -----------------
    Extract the contents from
    pdfs.
    """

    def __init__(self, file_path: str) -> None:

        self.file_path: str = file_path
        self.files: list = []
        self.pdf_mapping: dict = {}
        self.mapping_file_df: pd.DataFrame = None

        # - read in the names of the pdfs
        if os.path.isdir(self.file_path):
            self.files = list(filter(lambda x: os.path.splitext(x)[1] == ".pdf",
                                list(map(lambda y: os.path.join(self.file_path, y),
                                        os.listdir(self.file_path)))))

    def load_mapping_file(self, mapping_file_name: str) -> None:
        """
        Load the mapping file
        :param mapping_file_name:
        :return:
        """
        if os.path.isfile(os.path.join(self.file_path, mapping_file_name)):

            # - create a pandas DataFrame from the mapping file
            mapping_df = pd.read_csv(
                filepath_or_buffer=os.path.join(self.file_path, mapping_file_name),
                usecols=["document_date","loss_date", "object_name",
                         "Formats","Files" ,"acl_domain",
                         "category", "sub_category", "business_segment",
                         "cedent_common_nm", "cedent_id", "cedent_ultimate_parent_id",
                         "cedent_parent_common_nm", "business_type", "cedent_business_ref_nm",
                         "subdivision", "division", "claim_id"],
                engine='c',
                #index_col="Files",
            ).dropna(how='all')

            # - grab the file name and claim_id
            claims_df = (mapping_df.loc[:, ["Files", "claim_id"]]).dropna(how='all')
            # - grab the pdf documents
            self.mapping_file_df = (claims_df.loc[claims_df['Files'].str.endswith(".pdf")])
            #pprint(self.mapping_file_df.head(10))


    def extract_pdf_contents(self, num_files_to_read=1) -> None:
        """
        Extract the contents of each of
        the pdfs.

        :return:
        """
        counter = 0
        if self.files is not None:
            while counter < num_files_to_read and num_files_to_read < len(self.files):
                try:
                    # - fix strange character sequence found in some of the file names
                    current_pdf = self.files[counter].replace('~$', 'Re')
                    with open(current_pdf, 'rb') as pdf_file:
                        pdf = PdfFileReader(pdf_file)
                        num_pages = pdf.getNumPages() # get the number of pages in the pdf
                        pg = 0 # page counter
                        pdf_contents = [] # list to hold the current pdf's contents
                        while pg < num_pages: # iterate through each page of the pdf
                            current_page = pdf.getPage(pg)
                            # - extract current page's text and coerce into a string
                            document_contents = ''.join(
                                list(str(current_page.extractText()).splitlines())
                            )
                            pdf_contents.append(document_contents) # append to the temporary list
                            pg += 1 # increment page counter
                        # - write the pdf's contents to the pdf mapping
                        if current_pdf not in list(self.pdf_mapping.keys()):
                            self.pdf_mapping[str(self.files[counter])] = pdf_contents
                except:
                    print("Pdf: {} is malformed. Please open pdf to inspect.".format(str(self.files[counter])))
                counter += 1  # increment the pdf counter
            #pprint(self.pdf_mapping) # UNCOMMENT TO CHECK OUTPUT

    def clean_pdf_contents(self, mapping_key: str) -> pd.DataFrame:
        """
        Clean pdf contents.
        Files That Need a Different Text Extraction Method:
            - 2019 01 17 Atkinson Letter.pdf
            - Global Liberty July Billing XOL Claims.pdf
            - Oklahoma Police Report.pdf
        Keys:

        :return:
        """
        sub_mapping = {}

        keys_to_skip = [
            "Z:",
            "Z:"
        ]

        for k in list(self.pdf_mapping.keys()):
            #print("Current file: {}".format(k))
            if k not in keys_to_skip: # these keys require a different text extraction method
                # - transform the text into a usable format
                processed_text = self.pdf_mapping.get(k)
                joined_text = ''.join(p for p in processed_text)
                joined_text = PUNCTUATION.sub("", joined_text)
                joined_text = SPACES.sub(" ", joined_text)
                joined_text = ''.join(s for s in joined_text
                                      if s not in string.punctuation)

                # - overwrite the data in pdf_mapping
                self.pdf_mapping[k] = joined_text.lower()
                # - update the sub-mapping
                sub_mapping[k] = joined_text.lower()


        sub_mapping_df = pd.DataFrame.from_dict(sub_mapping, orient='index', columns=['raw_wordlist'])
        #pprint(sub_mapping_df)
        # - create a pandas DatFrame and write to csv
        if self.mapping_file_df is not None:
            # - grab the claim_id Series from mapping_file_df
            new_index = list(map(lambda x: os.path.split(x)[-1], list(sub_mapping_df.index)))

            # - reset the sub mapping filenames
            sub_mapping_files_df = pd.Series(new_index, name='Files', index=new_index) \
                                                .to_frame(name='filename')

            # - reset the sub mapping raw word list df
            sub_mapping_raw_wordlist_df = sub_mapping_df['raw_wordlist'] \
                                                .to_frame('raw_wordlist') \
                                                .reset_index(drop=True)
            # - reset the index
            sub_mapping_raw_wordlist_df.index = new_index

            # - grab the claim ids from the mapping file dataframe
            claim_id_df = self.mapping_file_df['claim_id'].to_frame('Claim_id')
            claim_id_df.index = self.mapping_file_df['Files']

            # - final pandas DataFrame to be returned
            final_df = claim_id_df.join(
                        sub_mapping_files_df.join(
                                sub_mapping_raw_wordlist_df, how='inner'),
                        how='inner')

            return final_df

    def write_pdf_contents_to_csv(self, pandas_df: pd.DataFrame, write_dir: str, write_name: str) -> None:
        """
        Write the pdf contents to a csv file.
        :return:
        """
        try:
            date = str(datetime.today())[:10].replace('-', '_')
            f_name = write_name + '_' + 'pdf' + '_' + date + '.csv'
            output = os.path.join(write_dir, f_name)
            # - write the output to
            pandas_df.to_csv(output)
        except:
            pass
        finally:
            pass

    def ocr_pdf_files(self, file_dir: str, *pdf_files):
        """
        Use optical character recognition for
        pdf image files.
        Files That Need a OCR:
            - 2019 01 17 Atkinson Letter.pdf
            - Global Liberty July Billing XOL Claims.pdf
            - Oklahoma Police Report.pdf
        Keys:
            - Z:\\
            - Z:\\
            - Z:\\
        :return:
        """
        # - list of pdf_img_files and join to directory path
        pdf_img_files = list(map(lambda x: os.path.join(file_dir, x),
                                    [x for x in pdf_files]))

        # - load image pdf
        pdf = open(pdf_img_files[0], 'rb')
        pdf_jpeg = "C:\\"
        img = pytesseract.image_to_string(pdf_jpeg)
















pdf_key = 'Z:\\'
mapping_file_name = "re_claim_doc_pcus.csv"
write_path = "N:\\"

extract_pdfs = ExtractPdfContent(file_path=file_path)
extract_pdfs.extract_pdf_contents(num_files_to_read=9)
extract_pdfs.load_mapping_file(mapping_file_name=mapping_file_name)
export_df = extract_pdfs.clean_pdf_contents(mapping_key=pdf_key)
#extract_pdfs.write_pdf_contents_to_csv(export_df, write_dir=write_path, write_name='non_ocr_pdf_files')
#pprint(export_df)
#pprint(list(extract_pdfs.pdf_mapping.keys()))
extract_pdfs.ocr_pdf_files(file_path, '2019 01 17 Atkinson Letter.pdf',
                           'Global Liberty July Billing XOL Claims.pdf',
                           'Oklahoma Police Report.pdf')

