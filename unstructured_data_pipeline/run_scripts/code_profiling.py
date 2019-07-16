"""
code_profiling
~~~~~~~~~~~~~~
This module is used to check for slow downs within the data pipeline.

# Run Times #
-------------
(7/1/2019): run_dms runtime: 804.3725 seconds for 100 files.

# Eml Run times #
-----------------
Eml Runtime 1: 0.5115971565246582 seconds/100 files
Eml Runtime 2: 0.40596508979797363 seconds/100 files
Eml Runtime 3: 0.3998758792877197 seconds/100 files
Eml Runtime 4: 0.4188268184661865 seconds/100 files


# Eml Run Times #
-----------------
# Data Set: claim_narrative_docs #
Eml Runtime 1: 383.476637840271 seconds/6422 .emls with print
Eml Runtime 2: 333.4942481517792 seconds/6422 .emls
Eml Runtime 3: 326.9785671234131 seconds/6422 .emls
Eml Runtime 4: 328.37847113609314 seconds/6422 .emls

# Rtf Run Times #
-----------------
# Data Set: claim_narrative_docs #
Docx Runtime 1: 17.929723978042603 seconds/279 .docx files
Docx Runtime 2: 18.184216499328613 seconds/279 .docx files
Docx Runtime 3: 18.40775966644287 seconds/279 .docx files
Docx Runtime 4: 18.152342557907104 seconds/279 .docx files


# Doc Run Times #
-----------------
# Data Set: claim_narrative_docs #
Doc Runtime 1: 463.0820550918579 seconds/3729 .doc files
Doc Runtime 2: 451.5370452404022 seconds/3729 .doc files
Doc Runtime 3: 453.0029327869415 seconds/3729 .doc files
Doc Runtime 4: 447.59998869895935 seconds/3729 .doc files

# Pdf Run Times #
-----------------
# Data Set: raw_data_6_19 #
Pdf Runtime 1: 204.69063806533813 seconds/12 .doc files
Pdf Runtime: 203.60551595687866 seconds/12 .doc files

"""
import time
from pprint import pprint
from unstructured_data_pipeline.data_mining.data_parsers import (RtfParser, EmlParser, FileGenerator,
                                                                 DocParser, DocxParser, PdfParser)
from unstructured_data_pipeline.run_scripts.run_dms import run_pdf_parser_v_1_1
from unstructured_data_pipeline.admin.file_stats import create_admin_spreadsheet, get_file_count_by_extension
claim_narrative_docs = r"X:\PC_BusinessAnalytics\Personal_Umbrella\Claims_Narrative_Docs"
raw_data_6_19 = r'V:\Dev\Delta\20190619\Document'
raw_data_5_21 = r'V:\Dev\Historical\20190521\Document'
admin_log_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\admin_log'

def profile_eml_parser(full_file_path: str):
    """Profile the eml parser"""
    try:
        start = time.time()
        file_gen = FileGenerator(project_file_path=full_file_path, file_ext='eml')
        file_iter = file_gen.__iter__()
        eml_parser = EmlParser()
        while True:
            try:
                eml_parser.extract_text(current_file=next(file_iter))
            except StopIteration:
                print('finished processing eml files')
                break
        end = time.time()
        print(f"Eml Runtime: {end-start}")
        print(f"Eml file counter: {eml_parser.file_counter}")
        print(f"Eml error file counter: {eml_parser.error_file_counter}")
        pprint(f"Eml Content: {eml_parser.mapping_dict.keys()}")
        create_admin_spreadsheet(write_to_path=admin_log_path, file_type='eml',
                                 count_extracted=eml_parser.file_counter,
                                 count_failed=eml_parser.error_file_counter,
                                 failed_file_name=eml_parser.error_files,
                                 failed_file_path=full_file_path,
                                 count=get_file_count_by_extension(file_path=full_file_path, file_ext='eml'))
    except Exception as e:
        print(e)

# create_admin_spreadsheet(write_to_path=r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\admin_log',
#                          file_type='eml', count_extracted=eml_files, count_failed=1,
#                          failed_file_name='', failed_file_path='', count=eml_files)

def profile_rtf_parser(full_file_path: str):
    """Profile the rtf parser"""
    try:
        start = time.time()
        file_gen = FileGenerator(project_file_path=full_file_path, file_ext='rtf')
        file_iter = file_gen.__iter__()
        rtf_parser = RtfParser()
        while True:
            try:
                rtf_parser.extract_text(current_file=next(file_iter))
            except StopIteration:
                print('finished processing rtf files')
                break
        end = time.time()
        print(f"Rtf Runtime: {end - start}")

        print(f"Rtfl file counter: {rtf_parser.file_counter}")
        print(f"Rtf error file counter: {rtf_parser.error_file_counter}")
        pprint(f"Rtf Content: {rtf_parser.mapping_dict.keys()}")
        create_admin_spreadsheet(write_to_path=admin_log_path, file_type='rtf',
                                 count_extracted=rtf_parser.file_counter,
                                 count_failed=rtf_parser.error_file_counter,
                                 failed_file_name=rtf_parser.error_files,
                                 failed_file_path=full_file_path,
                                 count=get_file_count_by_extension(file_path=full_file_path, file_ext='rtf'))
    except Exception as e:
        print(e)

def profile_doc_parser(full_file_path: str):
    """Profile the doc parser"""
    try:
        start = time.time()
        r_path = r'C:\Users\wmurphy\Desktop\R_Scripts'
        r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
        r_script = r'doc_to_csv_4_29_19.R'
        doc_to_csv_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\doc_to_csv'
        doc_parser = DocParser(r_executable=r_exacutable, r_path=r_path, r_script=r_script)
        # convert doc files to csv and write them to the doc_to_csv path
        doc_parser.run_doc_to_csv_r_script(file_path=full_file_path, timeout=str(20))
        file_gen = FileGenerator(project_file_path=doc_to_csv_path, file_ext='csv')
        file_iter = file_gen.__iter__()
        while True:
            try:
                current = doc_parser.extract_text(current_file=next(file_iter))
                print(current)
            except StopIteration:
                print('finished processing doc files')
                break
        doc_parser.remove_temp_doc_to_csv_files(doc_to_csv_write_path=doc_to_csv_path)
        end = time.time()
        print(f"Doc Runtime: {end - start}")
        print(f"Doc file counter: {doc_parser.file_counter}")
        print(f"Doc error file counter: {doc_parser.error_file_counter}")
        pprint(f"Doc Content: {doc_parser.mapping_dict.keys()}")
        create_admin_spreadsheet(write_to_path=admin_log_path, file_type='doc',
                                 count_extracted=doc_parser.file_counter,
                                 count_failed=doc_parser.error_file_counter,
                                 failed_file_name=doc_parser.error_files,
                                 failed_file_path=full_file_path,
                                 count=get_file_count_by_extension(file_path=full_file_path, file_ext='doc'))
        #print(f"Doc Content: {doc_parser.mapping_dict}")
    except Exception as e:
        print(e)


def profile_docx_parser(full_file_path: str):
    """Profile the docx parser"""
    try:
        start = time.time()
        file_gen = FileGenerator(project_file_path=full_file_path, file_ext='docx')
        file_iter = file_gen.__iter__()
        docx_parser = DocxParser()
        while True:
            try:
                docx_parser.extract_text(current_file=next(file_iter))
            except StopIteration:
                print('finished processing docx files')
                break
        end = time.time()
        print(f"Docx Runtime: {end - start}")
        #print(f"Docx Content: {docx_parser.mapping_dict}")
    except Exception as e:
        print(e)

def profile_pdf_parser():
    """Profile the pdf parser"""
    try:
        start = time.time()
        run_pdf_parser_v_1_1(project_file_path=claim_narrative_docs)
        end = time.time()
        print('finished processing pdf files')
        print(f"Pdf Runtime: {end - start}")
    except Exception as e:
        print(e)


def main():
    #profile_pdf_parser()
    #profile_eml_parser(full_file_path=raw_data_6_19)
    #profile_rtf_parser(full_file_path=raw_data_6_19)
    profile_doc_parser(full_file_path=raw_data_6_19)

if __name__ == '__main__':
    main()