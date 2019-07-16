"""
run_dms
~~~~~~~
This module serves as the run script that is controlled by an external powershell script
that runs based on a scheduled.

The current document types that are being text mined are:
    - PDF
    - RTF
    - DOC
    - DOCX
    - EML
More file types will be added in the future.

The pipeline works as follows:
   - First, the application walks through the specified directory, and extracts text from the specified file type.

   - From there, the extracted text is cleaned and stored in a python dictionary: {'filename', extracted_text}, which
    is then serialized using the pickle module and then sent to the remote linux server.

   - Then the mapping file is loaded into a python dictionary, serialized and sent to the remote linux server.

   -

"""
import os
import time
import threading
import multiprocessing
from pprint import pprint
from configparser import ConfigParser
from unstructured_data_pipeline.data_mining.data_parsers import (FileGenerator, EmlParser, RtfParser,
                                            DocxParser, DocParser, PdfParser, d)
from unstructured_data_pipeline.data_mining.concrete_ocr import PdfOCR
from unstructured_data_pipeline.data_mining.pdf_ocr_script_two import PdfOcrTwo
from unstructured_data_pipeline.data_mining.data_serialization import serialize_data, DataSerialization
from unstructured_data_pipeline.ssh_sftp_connections.hive_server import SftpConnection, RunRemoteScript
from unstructured_data_pipeline.data_mining.meta_data import MetaData
from unstructured_data_pipeline.admin.file_stats import get_file_count_by_extension
from unstructured_data_pipeline.admin.file_stats import get_unique_file_types
from unstructured_data_pipeline.admin.file_stats import create_admin_spreadsheet, get_file_count_by_extension
from unstructured_data_pipeline.data_mining.data_parsers import d
from unstructured_data_pipeline.logging.logging_config import BaseLogger
from unstructured_data_pipeline.config_path import config_file_path
from unstructured_data_pipeline.config_path import config_file_path
from unstructured_data_pipeline.data_mining.metadata_config import load_document_path, load_metadata_file
from unstructured_data_pipeline.data_mining.metadata_config import (DELTA_PROD_LOG, PROD_DELTA, PROD_HIST,
                                                                    DELTA_DEV_LOG, DEV_HIST, HIST_PROD_LOG)
from unstructured_data_pipeline.data_mining.check_metadata import load_meta_data
from unstructured_data_pipeline.email.pipeline_email import PipelineEmailer, create_email, python_path, py_path, test_path, t_path

# setup logging configuration
config = ConfigParser()
config.read(config_file_path)
sections = config.sections()
logfile = "dms_unst_pipeline_log_" + d
logger = BaseLogger(config.get(sections[2], 'DMS_LOGGING'), logfile)
logger.config() # use the default logging configuration

# admin log file path
admin_dir = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\admin_log'


def run_eml_parser(project_file_path: str, metadata_file=None):
    """Run the eml data processing class application"""
    try:
        # NOTE: updated *6/27/2019*
        # step 1: get the unloaded documents from the current project directory



        file_gen = FileGenerator(project_file_path=project_file_path, file_ext='eml')
        file_iter = file_gen.__iter__()
        eml_parser = EmlParser()
        while True:
            try:
                eml_parser.extract_text(current_file=next(file_iter))
            except StopIteration:
                print('finished processing eml files')
                break
        print(f"Number of Eml files processed: {eml_parser.file_counter}")
        print(f"Number of Eml error files: {eml_parser.error_file_counter}")
        print(f"Mapping Dict: {eml_parser.mapping_dict}")
        serialize_data(
            data_set=eml_parser.mapping_dict,
            write_name='EmlParserMappingDict'
        )
        try:
            # step 5: sftp the serialized mapping data into the remote server
            data_serialization = DataSerialization()  # create an instance of the DataSerialization class
            remote_pkl_path = data_serialization.get_remote_pickle_path()  # get the remote pickle path
            local_pkl_path = data_serialization.get_local_pickle_path()  # get the local pickle path
            sftp_connection = SftpConnection()  # create an instance of the SftpConnection class
            sftp_connection.connect(  # connect and load serialized data to the remote server
                filename=f'EmlParserMappingDict_{d}.pickle',
                filepath=local_pkl_path,
                remote_path=remote_pkl_path
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 6: load in the meta data pandas DataFrame and sftp to the remote server
            md_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
            md_file = 'metadata_log_06_26_2019'
            metadata_test_file_path = r'V:\Dev\Delta\20190619\Metadata'
            metadata_test_file = r'20190619.xls'
            meta_data = MetaData(metadata_log_file=os.path.join(md_log_path, md_file))
            meta_data_dict = meta_data.load_metadata_file(  # get the meta_data dictionary
                full_file_path=os.path.join(metadata_test_file_path, metadata_test_file)
            )
            print(f'Meta Data Dict := {meta_data_dict}')
            # # step 6.1: extract only the necessary columns from the meta_data data frame
            # md_df = meta_data_df.loc[:,['claim_id', 'Object_id', 'File_Name', 'Format',
            #                          'Version', 'Object_name', 'Prior_Version_Object_Name']]
            # step 6.2: serialize the data frame and sftp it to the remote server
            local_mapping_path = data_serialization.get_local_mapping_file_path()
            remote_mapping_path = data_serialization.get_remote_mapping_file_path()
            serialize_data(  # serialize the pickled meta data, data frame
                # NOTE: *updated 06/27/2019*
                # pass in the path from metadata_config.load_metadata_file function
                data_set=meta_data_dict,
                is_local=True,
                write_name='MetaData_DataFrame',
                is_pickle=False
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 7: create a new connection and load in the mapping data
            new_sftp_connection = SftpConnection()
            new_sftp_connection.connect(  # connect to the remote server and dump the pickled meta data, data frame
                filename=f'MetaData_DataFrame_{d}.pickle',
                filepath=local_mapping_path,
                remote_path=remote_mapping_path
            )

            # TEST: check the output of the file counter, error file counter and error files
            print(f"EML File count in filesystem: {get_file_count_by_extension(file_path=project_file_path, file_ext='eml')}")
            print(f"EML Error files list: {eml_parser.error_files}")
            print(f"EML Total files: {eml_parser.file_counter}")
            print(f"EML Total error files: {eml_parser.error_file_counter}")

            # step 8: update the admin file
            create_admin_spreadsheet(
                write_to_path=admin_dir, file_type='eml',
                count=get_file_count_by_extension(file_path=project_file_path, file_ext='eml'),
                count_extracted=eml_parser.file_counter, count_failed=eml_parser.error_file_counter,
                failed_file_path=project_file_path, failed_file_name=eml_parser.error_files
            )
            # step 8: execute the server side portion of the data pipeline
            remote_connection = RunRemoteScript(make_connection=True)  # connect to the remote server
            # step 8.1: execute the server side pipeline
            remote_connection.execute_server_side_pipeline()
        except Exception as e:
            logger.error(error=e)
    except (OSError, Exception) as e:
        logger.error(error=e)


def run_rtf_parser(project_file_path: str, metadata_file=None):
    raw_data = r'V:\Dev\Historical\20190521\Document'
    r_path = r'C:\Users\wmurphy\Desktop\R_Scripts'
    r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
    r_script = r'doc_to_csv_4_29_19.R'
    try:
        file_gen = FileGenerator(project_file_path=project_file_path, file_ext='rtf')
        file_iter = file_gen.__iter__()
        rtf_parser = RtfParser()
        while True:
            try:
                print(f"RTF: Extracting Text.")
                rtf_parser.extract_text(current_file=next(file_iter))
            except StopIteration:
                print('finished processing rtf files')
                break
        # print(f"Number of Rtf files processed: {rtf_parser.file_counter}")
        # print(f"Number of Rtf error files: {rtf_parser.error_file_counter}")
        # print(f"Mapping Dict: {rtf_parser.mapping_dict}")
        serialize_data(
            data_set=rtf_parser.mapping_dict,
            write_name='RtfParserMappingDict'
        )
        try:
            # step 5: sftp the serialized mapping data into the remote server
            data_serialization = DataSerialization()  # create an instance of the DataSerialization class
            remote_pkl_path = data_serialization.get_remote_pickle_path()  # get the remote pickle path
            local_pkl_path = data_serialization.get_local_pickle_path()  # get the local pickle path
            sftp_connection = SftpConnection()  # create an instance of the SftpConnection class
            sftp_connection.connect(  # connect and load serialized data to the remote server
                filename=f'RtfParserMappingDict_{d}.pickle',
                filepath=local_pkl_path,
                remote_path=remote_pkl_path
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 6: load in the meta data pandas DataFrame and sftp to the remote server
            md_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
            md_file = 'metadata_log_06_14_2019'
            metadata_test_file_path = r'V:\Dev\Delta\20190619\Metadata'
            metadata_test_file = r'20190619.xls'
            meta_data = MetaData(metadata_log_file=os.path.join(md_log_path, md_file))
            meta_data_dict = meta_data.load_metadata_file(  # get the meta_data data frame
                full_file_path=os.path.join(metadata_test_file_path, metadata_test_file)
            )
            # # step 6.1: extract only the necessary columns from the meta_data data frame
            # md_df = meta_data_df.loc[:,['claim_id', 'Object_id', 'File_Name', 'Format',
            #                          'Version', 'Object_name', 'Prior_Version_Object_Name']]
            # step 6.2: serialize the data frame and sftp it to the remote server
            local_mapping_path = data_serialization.get_local_mapping_file_path()
            remote_mapping_path = data_serialization.get_remote_mapping_file_path()
            serialize_data(  # serialize the pickled meta data, data frame
                data_set=meta_data_dict,
                write_name='MetaData_DataFrame',
                is_pickle=False
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 7: create a new connection and load in the mapping data
            new_sftp_connection = SftpConnection()
            new_sftp_connection.connect(  # connect to the remote server and dump the pickled meta data, data frame
                filename=f'MetaData_DataFrame_{d}.pickle',
                filepath=local_mapping_path,
                remote_path=remote_mapping_path
            )
            # step 8: update the admin file
            create_admin_spreadsheet(
                write_to_path=admin_dir, file_type='rtf',
                count=get_file_count_by_extension(file_path=project_file_path, file_ext='rtf'),
                count_extracted=rtf_parser.file_counter, count_failed=rtf_parser.error_file_counter,
                failed_file_path=project_file_path, failed_file_name=rtf_parser.error_files
            )
            # step 9: execute the server side portion of the data pipeline
            remote_connection = RunRemoteScript(make_connection=True)  # connect to the remote server
            # step 9.1: execute the server side pipeline
            remote_connection.execute_server_side_pipeline()
        except Exception as e:
            logger.error(error=e)
    except (OSError, Exception) as e:
        logger.error(error=e)

def run_docx_parser(project_file_path: str, metadata_file=None):
    raw_data = r'V:\Dev\Historical\20190521\Document'
    r_path = r'C:\Users\wmurphy\Desktop\R_Scripts'
    r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
    r_script = r'doc_to_csv_4_29_19.R'
    try:
        file_gen = FileGenerator(project_file_path=project_file_path, file_ext='docx')
        file_iter = file_gen.__iter__()
        docx_parser = DocxParser()
        while True:
            try:
                docx_parser.extract_text(current_file=next(file_iter))
            except StopIteration:
                print('finished processing rtf files')
                break
        # print(f"Number of Docx files processed: {docx_parser.file_counter}")
        # print(f"Number of Docx error files: {docx_parser.error_file_counter}")
        # print(f"Mapping Dict: {docx_parser.mapping_dict}")

        serialize_data(
            data_set=docx_parser.mapping_dict,
            write_name='DocxParserMappingDict'
        )
        try:
            # step 5: sftp the serialized mapping data into the remote server
            data_serialization = DataSerialization()  # create an instance of the DataSerialization class
            remote_pkl_path = data_serialization.get_remote_pickle_path()  # get the remote pickle path
            local_pkl_path = data_serialization.get_local_pickle_path()  # get the local pickle path
            sftp_connection = SftpConnection()  # create an instance of the SftpConnection class
            sftp_connection.connect(  # connect and load serialized data to the remote server
                filename=f'DocxParserMappingDict_{d}.pickle',
                filepath=local_pkl_path,
                remote_path=remote_pkl_path
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 6: load in the meta data pandas DataFrame and sftp to the remote server
            md_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
            md_file = 'metadata_log_06_14_2019'
            metadata_test_file_path = r'V:\Dev\Delta\20190619\Metadata'
            metadata_test_file = r'20190619.xls'
            meta_data = MetaData(metadata_log_file=os.path.join(md_log_path, md_file))
            meta_data_dict = meta_data.load_metadata_file(  # get the meta_data data frame
                full_file_path=os.path.join(metadata_test_file_path, metadata_test_file)
            )
            # # step 6.1: extract only the necessary columns from the meta_data data frame
            # md_df = meta_data_df.loc[:,['claim_id', 'Object_id', 'File_Name', 'Format',
            #                          'Version', 'Object_name', 'Prior_Version_Object_Name']]
            # step 6.2: serialize the data frame and sftp it to the remote server
            local_mapping_path = data_serialization.get_local_mapping_file_path()
            remote_mapping_path = data_serialization.get_remote_mapping_file_path()
            serialize_data(  # serialize the pickled meta data, data frame
                data_set=meta_data_dict,
                write_name='MetaData_DataFrame',
                is_pickle=False
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 7: create a new connection and load in the mapping data
            new_sftp_connection = SftpConnection()
            new_sftp_connection.connect(  # connect to the remote server and dump the pickled meta data, data frame
                filename=f'MetaData_DataFrame_{d}.pickle',
                filepath=local_mapping_path,
                remote_path=remote_mapping_path
            )
            # step 8: update the admin file
            create_admin_spreadsheet(
                write_to_path=admin_dir, file_type='docx',
                count=get_file_count_by_extension(file_path=project_file_path, file_ext='docx'),
                count_extracted=docx_parser.file_counter, count_failed=docx_parser.error_file_counter,
                failed_file_path=project_file_path, failed_file_name=docx_parser.error_files
            )

            # step 9: execute the server side portion of the data pipeline
            remote_connection = RunRemoteScript(make_connection=True)  # connect to the remote server
            # step 9.1: execute the server side pipeline
            remote_connection.execute_server_side_pipeline()
        except Exception as e:
            logger.error(error=e)
    except (OSError, Exception) as e:
        print(e)

def run_doc_parser(project_file_path: str, metadata_file=None):
    raw_data = r'V:\Dev\Historical\20190521\Document'
    r_path = r'C:\Users\wmurphy\Desktop\R_Scripts'
    r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
    r_script = r'doc_to_csv_4_29_19.R'
    doc_to_csv_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\doc_to_csv'
    try:
        # create an instance of the DocParser class
        doc_parser = DocParser(r_executable=r_exacutable, r_path=r_path, r_script=r_script)

        # convert doc files to csv and write them to the doc_to_csv path
        doc_parser.run_doc_to_csv_r_script(file_path=project_file_path, timeout=str(20))

        # NOTE: make sure this is pointed at the correct file location(doc_to_csv) path!
        file_gen = FileGenerator(project_file_path=doc_to_csv_path, file_ext='csv')
        file_iter = file_gen.__iter__()

        # extract the text from the converted doc files
        # NOTE: updated *06/27/19*
        # change current_file=doc_to_csv path since that's where the extracted txt files go
        while True:
            try:
                current = doc_parser.extract_text(current_file=next(file_iter))
                print(current)
            except StopIteration:
                print('finished processing rtf files')
                break
        # print(f"Number of Docx files processed: {doc_parser.file_counter}")
        # print(f"Number of Docx error files: {doc_parser.error_file_counter}")
        # print(f"Mapping Dict: {doc_parser.mapping_dict}")

        # attempt to remove all the temporary files
        #doc_parser.remove_temp_doc_to_csv_files(doc_to_csv_write_path=doc_to_csv_path)
        serialize_data(
            data_set=doc_parser.mapping_dict,
            write_name='DocParserMappingDict'
        )
        try:
            # step 5: sftp the serialized mapping data into the remote server
            data_serialization = DataSerialization()  # create an instance of the DataSerialization class
            remote_pkl_path = data_serialization.get_remote_pickle_path()  # get the remote pickle path
            local_pkl_path = data_serialization.get_local_pickle_path()  # get the local pickle path
            sftp_connection = SftpConnection()  # create an instance of the SftpConnection class
            sftp_connection.connect(  # connect and load serialized data to the remote server
                filename=f'DocParserMappingDict_{d}.pickle',
                filepath=local_pkl_path,
                remote_path=remote_pkl_path
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 6: load in the meta data pandas DataFrame and sftp to the remote server
            md_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
            md_file = 'metadata_log_06_27_2019'
            metadata_test_file_path = r'V:\Dev\Delta\20190619\Metadata'
            metadata_test_file = r'20190619.xls'
            meta_data = MetaData(metadata_log_file=os.path.join(md_log_path, md_file))

            meta_data_dict = meta_data.load_metadata_file(  # get the meta_data data frame
                full_file_path=os.path.join(metadata_test_file_path, metadata_test_file)
            )
            # # step 6.1: extract only the necessary columns from the meta_data data frame
            # md_df = meta_data_df.loc[:,['claim_id', 'Object_id', 'File_Name', 'Format',
            #                          'Version', 'Object_name', 'Prior_Version_Object_Name']]
            # step 6.2: serialize the data frame and sftp it to the remote server
            local_mapping_path = data_serialization.get_local_mapping_file_path()
            remote_mapping_path = data_serialization.get_remote_mapping_file_path()
            serialize_data(  # serialize the pickled meta data, data frame
                data_set=meta_data_dict,
                write_name='MetaData_DataFrame',
                is_pickle=False
            )
        except Exception as e:
            logger.error(error=e)
        try:
            # step 7: create a new connection and load in the mapping data
            new_sftp_connection = SftpConnection()
            new_sftp_connection.connect(  # connect to the remote server and dump the pickled meta data, data frame
                filename=f'MetaData_DataFrame_{d}.pickle',
                filepath=local_mapping_path,
                remote_path=remote_mapping_path
            )

            # step 8: update the admin file
            create_admin_spreadsheet(
                write_to_path=admin_dir, file_type='doc',
                count=get_file_count_by_extension(file_path=project_file_path, file_ext='doc'),
                count_extracted=doc_parser.file_counter, count_failed=doc_parser.error_file_counter,
                failed_file_path=project_file_path, failed_file_name=doc_parser.error_files
            )
            # step 9: execute the server side portion of the data pipeline
            remote_connection = RunRemoteScript(make_connection=True)  # connect to the remote server
            # step 9.1: execute the server side pipeline
            remote_connection.execute_server_side_pipeline()
        except Exception as e:
            logger.error(error=e)
    except Exception as e:
        print(e)

def process_pdf_img_algo() -> list:
    """
    This algorithm ocr's the pdf image files and creates a temporary data structure
    that is used to properly insert the extracted text into the correct page position
    in the PdfParser mapping_container data structure.

    :returns
        pdf_name_list :type list
        temp :type dict[dict[str]]
    """
    pdf_ocr_two = PdfOcrTwo()   # create an instance of the pdf image ocr class

    # path to image pdf image files
    pdf_img_path = os.path.join(
        os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\pdf_img'
    )

    # temp is the data structure that stores the extracted text and processes it by page
    temp: dict[dict[list]] = {}  # store the current pages image text if page has multiple images
    pdf_name_list: list = []     # store the extracted pdf names to be used in the pdf parser run script
    for f in os.listdir(pdf_img_path):
        # walk through the mapping container to check that image file names and mapping filenames are equivalent
        current_img_pdf = os.path.splitext(f)       # list of current pdf's image files
        pdf_img_content = current_img_pdf[0].split('_')
        pdf_name = current_img_pdf[0].split('.')[0]
        pg = pdf_img_content[-2]

        if pdf_name not in pdf_name_list:
            # insert the current pdf name into the pdf_name_list
            pdf_name_list.append(pdf_name)

        if pdf_name not in temp:
            # step 1: add the new pdf to the temp dictionary
            temp[pdf_name] = {}
            if pg not in temp[pdf_name]:
                # step 1.2: add the current page to the dict[dict[list]]
                temp[pdf_name][pg] = []

                # step 1.3: ocr the image file and insert the text into temp
                text = pdf_ocr_two.ocr_image_file(current_img_file=os.path.join(pdf_img_path, f))
                temp[pdf_name][pg].append(text)

            elif pg in temp[pdf_name].keys():
                # step 1.4: if the current pg is already contained int temp, append the extracted text
                text = pdf_ocr_two.ocr_image_file(current_img_file=os.path.join(pdf_img_path, f))
                temp[pdf_name][pg].append(text)

        elif pdf_name in temp.keys():
            # step 2: if the current pdf is already contained in temp
            if pg not in temp[pdf_name].keys():
                # step 2.1: if the current page is not a key in temp, add the new page to temp
                temp[pdf_name][pg] = []
                # step 2.2: extract the current images text and append it to the current pdf's page list
                text = pdf_ocr_two.ocr_image_file(current_img_file=os.path.join(pdf_img_path, f))
                temp[pdf_name][pg].append(text)
            elif pg in temp[pdf_name].keys():
                # step 2.3: append to the current pdf's page list with the image's extracted text
                text = pdf_ocr_two.ocr_image_file(current_img_file=os.path.join(pdf_img_path, f))
                temp[pdf_name][pg].append(text)

    # step 3: join the text contained within temp[pdf_name][pg] lists
    for f in list(temp.keys()):
        current = temp[f]
        for p in list(current.keys()):
            joined_text = ''.join(t for t in current[p])
            current[p] = joined_text

    # return a list containing the pdf file names and the temp data structure
    return [pdf_name_list, temp]


def run_pdf_parser_v_1_1(project_file_path: str, metadata_file=None):
    pdf_parser = PdfParser()    # create an instance of the PdfParser class
    file_gen = FileGenerator(
        project_file_path=project_file_path, # create an instance of the FileGenerator class
        file_ext='pdf'
    )
    file_iter = file_gen.__iter__() # create a file iterator
    pdf_ocr_two = PdfOcrTwo()

    while True:
        # step 1: iterate through the pdfs
        try:
            current_pdf = next(file_iter)   #  get the current pdf
            pdf_parser.extract_text(current_file=current_pdf)   # extract any text
            pdf_ocr_two.extract_img_minecart(full_file_name=current_pdf)   # extract pdf images
        except StopIteration:
            print("finished processing pdf files")
            break

    # step 2: run the pdf image algorithm
    pdf_name_list, pdf_img_data_struct = process_pdf_img_algo()

    for n in pdf_img_data_struct.keys():
        # step 3: get the current pdf image file name
        for m in pdf_parser.mapping_container:
            # step 3.1: search the mapping container for a matching file name
            fn = m['file_name'].split('.')[0]   # format the filename
            if n == fn:
                # step 3.2: if names match, search through the pdf img container for a matching pg
                for pg in pdf_img_data_struct[n].keys():
                    pg = int(pg) # convert to the same type as mapping_container pages
                    if pg in list(m.keys())[1:]:
                        # step 3.3: join the text and insert to the correct pdf's page
                        full_text = m[pg] + pdf_img_data_struct[n][str(pg)]
                        m[pg] = full_text
    pprint(f"PDF Text: {pdf_parser.mapping_container}")
    # NOTE: Comment back in after tests
    # step 4: write the mapping file to a pickle file and save in the pickle file directory
    serialize_data(
        data_set=pdf_parser.mapping_container,
        write_name='PdfParserMappingContainer'
    )
    try:
        # step 5: sftp the serialized mapping data into the remote server
        data_serialization = DataSerialization()    # create an instance of the DataSerialization class
        remote_pkl_path = data_serialization.get_remote_pickle_path()   # get the remote pickle path
        local_pkl_path = data_serialization.get_local_pickle_path()     # get the local pickle path
        sftp_connection = SftpConnection()  # create an instance of the SftpConnection class
        sftp_connection.connect(    # connect and load serialized data to the remote server
            filename=f'PdfParserMappingContainer_{d}.pickle',
            filepath=local_pkl_path,
            remote_path=remote_pkl_path
        )
    except Exception as e:
        logger.error(error=e)
    try:
        # step 6: load in the meta data pandas DataFrame and sftp to the remote server
        md_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\metadata_log'
        md_file = 'metadata_log_06_14_2019'
        metadata_test_file_path = r'V:\Dev\Delta\20190619\Metadata'
        metadata_test_file = r'20190619.xls'
        meta_data = MetaData(metadata_log_file=os.path.join(md_log_path, md_file))
        meta_data_dict= meta_data.load_metadata_file(    # get the meta_data data frame
            full_file_path=os.path.join(metadata_test_file_path, metadata_test_file)
        )
        # # step 6.1: extract only the necessary columns from the meta_data data frame
        # md_df = meta_data_df.loc[:,['claim_id', 'Object_id', 'File_Name', 'Format',
        #                          'Version', 'Object_name', 'Prior_Version_Object_Name']]
        # step 6.2: serialize the data frame and sftp it to the remote server
        local_mapping_path = data_serialization.get_local_mapping_file_path()
        remote_mapping_path = data_serialization.get_remote_mapping_file_path()
        serialize_data(     # serialize the pickled meta data, data frame
            data_set=meta_data_dict,
            write_name='MetaData_DataFrame',
            is_pickle=False
        )
    except Exception as e:
        logger.error(error=e)
    try:
        # step 7: create a new connection and load in the mapping data
        new_sftp_connection = SftpConnection()
        new_sftp_connection.connect(    # connect to the remote server and dump the pickled meta data, data frame
            filename=f'MetaData_DataFrame_{d}.pickle',
            filepath=local_mapping_path,
            remote_path=remote_mapping_path
        )
        # step 8: update the admin file
        create_admin_spreadsheet(
            write_to_path=admin_dir, file_type='pdf',
            count=get_file_count_by_extension(file_path=project_file_path, file_ext='doc'),
            count_extracted=pdf_parser.file_counter, count_failed=pdf_parser.error_file_counter,
            failed_file_path=project_file_path, failed_file_name=pdf_parser.error_files
        )
        # step 9: execute the server side portion of the data pipeline
        remote_connection = RunRemoteScript(make_connection=True) # connect to the remote server
        # step 9.1: execute the server side pipeline
        remote_connection.execute_server_side_pipeline()
    except Exception as e:
        logger.error(error=e)



def run_pdf_parser(project_file_path: str):
    file_gen = FileGenerator(project_file_path=project_file_path, file_ext='pdf')
    file_iter = file_gen.__iter__()
    pdf_parser = PdfParser()
    pdf_ocr = PdfOCR()
    pdf_ocr_two = PdfOcrTwo()
    ocr_write_path = os.path.join(
        os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\pdf_text'
    )
    write_path = os.path.join(
        os.environ['UNSTRUCTURED_PIPELINE_PATH'], 'DMS_Claims\pdf_img'
    )
    while True:
        try:
            pass
            current_pdf = next(file_iter)
            pdf_parser.extract_text(current_file=current_pdf)
            pdf_ocr_two.extract_img_minecart(full_file_name=current_pdf)
        except StopIteration:
            print("finished processing pdf files")
            break

    # walk through the mapping container to check that image file names and mapping filenames are equivalent
    temp = {}   # store the current pages image text if page has multiple images
    for f in os.listdir(write_path):
        current_img_pdf = os.path.splitext(f)
        pdf_pages = current_img_pdf[0].split('_')
        pg = pdf_pages[-2]      # grab the current page number
        sub_pg = pdf_pages[-1]  # grab the current img on the current page
        pdf_name = current_img_pdf[0].split('.')[0] # grab the pdf file's name

        if pdf_name not in temp.keys():
            # step 1: add the new pdf to the temp dictionary
            temp[pdf_name] = {} # insert the current pdf image name into the temp dictionary

            # step 1.1: extract the text from current page
            text = pdf_ocr_two.ocr_image_file(current_img_file=os.path.join(write_path, f))

            # step 1.2: update the current pages text information
            if pg not in temp[pdf_name].keys():
                temp[pdf_name][pg] = []
                temp[pdf_name][pg].append(text)

            # step 1.3: append the page's sub image text
            elif pg in temp[pdf_name].keys():
                temp[pdf_name][pg].append(text)

    # check the output of the ocr'ed data
    pprint(f"Temp Pdf Ocr Text: {temp}")


    # print(f"pdf_name = {pdf_name}")
    # print(f"pg = {pg}")
    # print(f"sub_page = {sub_pg}")
    # for m in pdf_parser.mapping_container:
    #     fn = m['file_name'].split('.')[0]
    #     if pdf_name == fn:
    #         text = pdf_ocr_two.extract_img_minecart(full_file_name=os.path.join(write_path, f))
    #pdf_info = pdf_parser.insert_new_pdf(current_file=next(file_iter))


def main():
    """Run the specified parser"""
    raw_data = r'V:\Dev\Historical\20190521\Document'
    raw_data_6_19 = r'V:\Dev\Delta\20190619\Document'
    meta_data_6_19 = r'V:\Dev\Delta\20190619\Metadata\20190619.xls'
    raw_data_6_20 = r'V:\Dev\Delta\20190620\Document'
    doc_to_csv_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\doc_to_csv'
    pu_raw_data = r'X:\PC_BusinessAnalytics\Personal_Umbrella\Claims_Narrative_Docs'
    r_scripts = r'C:\Users\wmurphy\Desktop\R Scripts'
    r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
    r_script = r'doc_to_csv_4_29_19.R'

    # NOTE: updated *06/27/2019*
    # added automation of selecting dms file
    prod_hist  = None
    #prod_delta_doc_dir, prod_delta_metadata_dict, current = load_meta_data(is_prod=False, is_historical=False)
    dev_hist   = None
    dev_delta  = None




    start = time.time() # track the application runtime
    # create a set of threads to run the unstructured data pipeline
    t1 = threading.Thread(target=run_eml_parser, args=(raw_data_6_19,))
    t2 = threading.Thread(target=run_rtf_parser, args=(raw_data_6_19,))
    t3 = threading.Thread(target=run_docx_parser, args=(raw_data_6_19,))
    t4 = threading.Thread(target=run_doc_parser, args=(raw_data_6_19,))
    t5 = threading.Thread(target=run_pdf_parser_v_1_1, args=(raw_data_6_19,))

    # start the threads
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()

    # join the threads
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()

    # start the email
    create_email(business_segment='NA Prop/Fac', python_path=py_path, hadoop_env=t_path,
                 code_version=1.0, hadoop_path='P:ath/to/hadoop', hive='MSEAULXDA03',
                 daily_win_total=100, dms_total=100, total_python=100, total_hdfs=99, total_hive=100)


    # run_eml_parser(project_file_path=raw_data_6_19)
    # run_rtf_parser(project_file_path=raw_data_6_19)
    # run_docx_parser(project_file_path=raw_data_6_19)
    # run_doc_parser(project_file_path=raw_data_6_19)
    # run_pdf_parser_v_1_1(project_file_path=raw_data_6_19)
    end = time.time()
    print(f"Finished Running the Unstructured Data Pipeline.\nThreaded Run\nRuntime: {end - start}")
if __name__ == '__main__':
    main()