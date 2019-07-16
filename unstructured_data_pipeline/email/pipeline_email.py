"""
pipeline_email
~~~~~~~~~~~~~~

"""
import os
import abc
import sys
import base64
import html
import smtplib
from pprint import pprint
from datetime import datetime
from optparse import OptionParser
from jinja2 import Template, FileSystemLoader, Environment, PackageLoader
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import chain
from functools import wraps, reduce, partial
from configparser import ConfigParser

from unstructured_data_pipeline.data_mining.data_parsers import d
from unstructured_data_pipeline.logging.logging_config import BaseLogger
from unstructured_data_pipeline.config_path import config_file_path
from unstructured_data_pipeline.ssh_sftp_connections.hive_server import RunRemoteScript
from unstructured_data_pipeline.admin.file_stats import get_file_count_by_extension
from unstructured_data_pipeline.data_mining.meta_data import MetaData
from unstructured_data_pipeline.data_mining.data_parsers import (FileGenerator, EmlParser, RtfParser,
                                                                 DocxParser, DocParser, PdfParser)
test_path = r'V:\Dev\Delta\20190619\Document'
# C:\Users\wmurphy\PycharmProjects\unstructured_data_pipeline_6_10_2019\unstructured_data_pipeline\run_scripts\run_dms.py

t_path = os.path.splitdrive(test_path)[1]
python_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims'
py_path = os.path.splitdrive(python_path)[1]
# setup logging configuration
config = ConfigParser()
config.read(config_file_path)
sections = config.sections()
logfile = "dms_unst_pipeline_log_" + d
logger = BaseLogger(config.get(sections[2], 'DMS_LOGGING'), logfile)
logger.config() # use the default logging configuration


class PipelineEmailer(object):
    """Concrete implementation of the email interface"""

    def __init__(self):
        self._host = None
        self._to: list = []
        self._from: str = None
        self._body: str = None
        self._subject: str = None
        self._attachments: str = None
        self.mail_server:smtplib.SMTP = None

    @property
    def host___(self):
        return self._host

    @host___.setter
    def host___(self, host: str):
        self._host = host

    @property
    def to___(self):
        return self._to

    @to___.setter
    def to___(self, *to):
        self._to = list(chain.from_iterable(list(to)))

    @property
    def from___(self):
        return self._from

    @from___.setter
    def from___(self, f: str):
        self._from = f

    @property
    def subject___(self):
        return self._subject

    @subject___.setter
    def subject___(self, subject: str):
        self._subject = subject

    @property
    def body___(self):
        return self._body

    @body___.setter
    def body___(self, body: str):
        self._body = body

    @property
    def attachments___(self):
        return self._attachments

    @attachments___.setter
    def attachments___(self, *attachments):
        pass

    def send_email(self):
        """Send an email"""
        try:
            # start the email server
            self.mail_server = smtplib.SMTP(self.host___)

            COMMASPACE = ', '
            # create the message
            message = MIMEText(self.body___, 'html')
            message['From'] = self.from___
            message['To'] = COMMASPACE.join(self.to___)
            message['Subject'] = self.subject___

            # send the email
            self.mail_server.sendmail(message['From'], message['To'], message.as_string())
        except Exception as e:
            logger.error(error=e)
            logger.error(error="An email error has occurred")
        finally:
            # close the message server
            self.mail_server.quit()


def create_email(business_segment: str, hadoop_env: str, python_path: str, code_version: float,
                 hadoop_path: str, hive: str, daily_win_total: int, dms_total: int,
                 total_python: int, total_hdfs: int, total_hive: int):
    """creates the email that will be sent out to end users"""
    # step1: setup jinja2 template setup
    html_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\versioned_code\unstructured_data_pipeline_07_11_19\templates'
    html_file = 'count_loc_type.html'
    file_loader = FileSystemLoader(html_path)
    environment = Environment(loader=file_loader)
    template = environment.get_template(html_file)

    # system file counts
    pdf_fs_count = get_file_count_by_extension(file_path=test_path,  file_ext='pdf')
    rtf_fs_count = get_file_count_by_extension(file_path=test_path,  file_ext='rtf')
    docx_fs_count = get_file_count_by_extension(file_path=test_path, file_ext='docx')
    doc_fs_count = get_file_count_by_extension(file_path=test_path,  file_ext='doc')
    eml_fs_count = get_file_count_by_extension(file_path=test_path,  file_ext='eml')

    # total supported system files
    total_supported_sys_files = pdf_fs_count + rtf_fs_count + docx_fs_count + doc_fs_count + eml_fs_count

    # hive counts
    remote_script = RunRemoteScript(make_connection=True)
    pdf_hive_count = remote_script.execute_hive_count(hive_table='pdfdata_2019_07_01') # get the count of files from hive
    rtf_hive_count = remote_script.execute_hive_count(hive_table='rtfdata_2019_07_01')
    docx_hive_count = remote_script.execute_hive_count(hive_table='docxdata_2019_07_01')
    doc_hive_count = remote_script.execute_hive_count(hive_table='docdata_2019_07_01')
    eml_hive_count = remote_script.execute_hive_count(hive_table='emldata_2019_07_01')

    # total hive count
    total_hive_count = int(pdf_hive_count) + int(rtf_hive_count) + int(docx_hive_count) + int(doc_hive_count) + int(eml_hive_count)

    # dms counts
    md_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
    md_file = 'metadata_log_06_14_2019'
    metadata_test_file_path = r'V:\Dev\Delta\20190619\Metadata'
    metadata_test_file = r'20190619.xls'
    meta_data = MetaData(metadata_log_file=os.path.join(md_log_path, md_file))
    meta_data_df = meta_data.load_metadata_as_df(os.path.join(metadata_test_file_path, metadata_test_file))

    pdf_dms_count = meta_data_df[meta_data_df['Format'] == 'pdf'].shape[0]
    rtf_dms_count = meta_data_df[meta_data_df['Format'] == 'rtf'].shape[0]
    docx_dms_count = meta_data_df[meta_data_df['Format'] == 'docx'].shape[0]
    doc_dms_count = meta_data_df[meta_data_df['Format'] == 'doc'].shape[0]
    eml_dms_count = meta_data_df[meta_data_df['Format'] == 'eml'].shape[0]

    # total dms count
    total_dms_count = pdf_dms_count + rtf_dms_count + docx_dms_count + doc_dms_count + eml_dms_count


    # python data mining count
    pdf_gen = FileGenerator(project_file_path=test_path, file_ext='pdf')
    pdf_iter = pdf_gen.__iter__()
    pdf_parser = PdfParser()
    while True:
        try:
            current = pdf_parser.extract_text(current_file=next(pdf_iter))
        except StopIteration:
            print('finished processing pdf files')
            break
    pdf_ocr_count = len(pdf_parser.mapping_container)

    rtf_gen = FileGenerator(project_file_path=test_path, file_ext='rtf')
    rtf_iter = rtf_gen.__iter__()
    rtf_parser = RtfParser()
    while True:
        try:
            current = rtf_parser.extract_text(current_file=next(rtf_iter))
        except StopIteration:
            print('finished processing rtf files')
            break
    rtf_ocr_count = len(rtf_parser.mapping_dict.keys())

    docx_gen = FileGenerator(project_file_path=test_path, file_ext='docx')
    docx_iter = docx_gen.__iter__()
    docx_parser = DocxParser()
    while True:
        try:
            current = docx_parser.extract_text(current_file=next(docx_iter))
        except StopIteration:
            print('finished processing docx files')
            break
    docx_ocr_count = len(docx_parser.mapping_dict.keys())

    eml_gen = FileGenerator(project_file_path=test_path, file_ext='eml')
    eml_iter = eml_gen.__iter__()
    eml_parser = EmlParser()
    while True:
        try:
            current = eml_parser.extract_text(current_file=next(eml_iter))
        except StopIteration:
            print('finished processing eml files')
            break
    eml_ocr_count = len(eml_parser.mapping_dict.keys())

    r_path = r'C:\Users\wmurphy\Desktop\R_Scripts'
    r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
    r_script = r'doc_to_csv_4_29_19.R'
    doc_to_csv_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\doc_to_csv'

    doc_gen = FileGenerator(project_file_path=test_path, file_ext='csv')
    doc_iter = doc_gen.__iter__()
    doc_parser = DocParser(r_executable=r_exacutable, r_path=r_path, r_script=r_script)
    doc_parser.run_doc_to_csv_r_script(file_path=test_path, timeout=str(20))
    while True:
        try:
            current = doc_parser.extract_text(current_file=next(doc_iter))
        except StopIteration:
            print('finished processing pdf files')
            break
    doc_ocr_count = len(doc_parser.mapping_dict.keys())

    # total ocr counts
    total_ocr_count = pdf_ocr_count + rtf_ocr_count + docx_ocr_count + doc_ocr_count + eml_ocr_count

    hive_tables = list(map(lambda x: x + '_' + str(d).replace('-', '_'),
                           ['docdata', 'docxdata', 'emldata', 'rtfdata', 'pdfdata']))


    output = template.render(
                             business_segment=business_segment,
                             hadoop_env=t_path, python_path=python_path,
                             code_version=code_version, linux_path='/genre/bda/apps/data_pipeline',
                             hive=hive, daily_win_total=total_supported_sys_files, dms_total=total_dms_count,
                             total_python=total_ocr_count, total_hdfs=total_hdfs,
                             total_hive=total_hive,
                             logfile_path=r'\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\logging',
                             hive_tables=hive_tables,
                             pdf_hive_count=pdf_hive_count, pdf_fs_count=pdf_fs_count, rtf_hive_count=rtf_hive_count,
                             rtf_fs_count=rtf_fs_count, eml_hive_count=eml_hive_count, eml_fs_count=eml_fs_count,
                             doc_hive_count=doc_hive_count, doc_fs_count=doc_fs_count, docx_hive_count=docx_hive_count,
                             docx_fs_count=docx_fs_count, pdf_dms_count=pdf_dms_count,
                             rtf_dms_count=rtf_dms_count, docx_dms_count=docx_dms_count,
                             doc_dms_count=doc_dms_count, eml_dms_count=eml_dms_count, pdf_ocr_count=pdf_ocr_count,
                             eml_ocr_count=eml_ocr_count, rtf_ocr_count=rtf_ocr_count, docx_ocr_count=docx_ocr_count,
                             doc_ocr_count=doc_ocr_count)
    pipeline_email = PipelineEmailer()
    pipeline_email.host___ = "mail.genre.com"
    #pipeline_email.to___ = ['Global_Analytics@genre.com']
    pipeline_email.to___ = ['william.murphy@genre.com']
    pipeline_email.body___ = output
    pipeline_email.from___ = 'william.murphy@genre.com'
    pipeline_email.subject___ = 'Python Emailer Test' + str(datetime.now())
    pipeline_email.send_email()


# if __name__ == '__main__':
#     python_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims'
#     py_path = os.path.splitdrive(python_path)[1]
#     create_email(business_segment='NA Prop/Fac', python_path=py_path, hadoop_env=t_path,
#                  code_version=1.0, hadoop_path='P:ath/to/hadoop', hive='MSEAULXDA03',
#                  daily_win_total=100, dms_total=100, total_python=100, total_hdfs=99, total_hive=100)