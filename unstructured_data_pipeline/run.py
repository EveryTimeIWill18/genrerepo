"""
run
~~~
"""
import logging
from pprint import pprint
from unstructured_data_pipeline.data_mining.data_parsers import (FileGenerator, EmlParser, RtfParser,
                                            DocxParser, DocParser, PdfParser)


def run_eml_parser(project_file_path: str):
    """Run the eml data processing class application"""
    try:
        file_gen = FileGenerator(project_file_path=project_file_path, file_ext='eml')
        file_iter = file_gen.__iter__()
        eml_parser = EmlParser()
        while True:
            try:
                current = eml_parser.extract_text(current_file=next(file_iter))
            except StopIteration:
                print('finished processing eml files')
                break
        print(f"Number of Eml files processed: {eml_parser.file_counter}")
        print(f"Number of Eml error files: {eml_parser.error_file_counter}")
        pprint(f"Mapping Dict: {eml_parser.mapping_dict}")
    except (OSError, Exception) as e:
        print(e)


def run_rtf_parser(project_file_path: str):
    try:
        file_gen = FileGenerator(project_file_path=project_file_path, file_ext='rtf')
        file_iter = file_gen.__iter__()
        rtf_parser = RtfParser()
        while True:
            try:
                current = rtf_parser.extract_text(current_file=next(file_iter))

            except StopIteration:
                print('finished processing rtf files')
                break
        print(f"Number of Rtf files processed: {rtf_parser.file_counter}")
        print(f"Number of Rtf error files: {rtf_parser.error_file_counter}")
        pprint(f"Mapping Dict: {rtf_parser.mapping_dict}")
    except (OSError, Exception) as e:
            print(e)

def run_docx_parser(project_file_path: str):
    try:
        file_gen = FileGenerator(project_file_path=project_file_path, file_ext='docx')
        file_iter = file_gen.__iter__()
        docx_parser = DocxParser()
        while True:
            try:
                current = docx_parser.extract_text(current_file=next(file_iter))

            except StopIteration:
                print('finished processing rtf files')
                break
        print(f"Number of Docx files processed: {docx_parser.file_counter}")
        print(f"Number of Docx error files: {docx_parser.error_file_counter}")
        pprint(f"Mapping Dict: {docx_parser.mapping_dict}")
    except (OSError, Exception) as e:
        print(e)

def run_doc_parser(project_file_path: str):
    raw_data = r'V:\Dev\Historical\20190521\Document'
    r_path = r'C:\Users\wmurphy\Desktop\R_Scripts'
    r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
    r_script = r'doc_to_csv_4_29_19.R'
    doc_to_csv_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\doc_to_csv'
    try:
        file_gen = FileGenerator(project_file_path=project_file_path, file_ext='csv')
        file_iter = file_gen.__iter__()
        doc_parser = DocParser(r_executable=r_exacutable, r_path=r_path, r_script=r_script)

        # convert doc files to csv
        doc_parser.run_doc_to_csv_r_script(file_path=raw_data, timeout=str(20))

        # extract the text from the converted doc files
        while True:
            try:
                current = doc_parser.extract_text(current_file=next(file_iter))
                pprint(current)
            except StopIteration:
                print('finished processing rtf files')
                break
        print(f"Number of Docx files processed: {doc_parser.file_counter}")
        print(f"Number of Docx error files: {doc_parser.error_file_counter}")
        pprint(f"Mapping Dict: {doc_parser.mapping_dict}")

        # attempt to remove all the temporary files
        doc_parser.remove_temp_doc_to_csv_files(doc_to_csv_write_path=doc_to_csv_path)

    except Exception as e:
        print(e)

def run_pdf_parser(project_file_path: str):
    file_gen = FileGenerator(project_file_path=project_file_path, file_ext='pdf')
    file_iter = file_gen.__iter__()
    pdf_parser = PdfParser()

    while True:
        try:
            pdf_parser.extract_text(current_file=next(file_iter))
        except StopIteration:
            print("finished processing pdf files")
            break
    print(f"Number of Docx files processed: {pdf_parser.file_counter}")
    print(f"Number of Docx error files: {pdf_parser.error_file_counter}")


    #pdf_info = pdf_parser.insert_new_pdf(current_file=next(file_iter))






def main():
    """Run the specified parser"""
    raw_data = r'V:\Dev\Historical\20190521\Document'
    doc_to_csv_path = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\doc_to_csv'
    pu_raw_data = r'X:\PC_BusinessAnalytics\Personal_Umbrella\Claims_Narrative_Docs'
    r_scripts = r'C:\Users\wmurphy\Desktop\R Scripts'
    r_exacutable = r'C:\Program Files\R\R-3.5.3\bin\Rscript'
    r_script = r'doc_to_csv_4_29_19.R'



    #run_eml_parser(project_file_path=raw_data)
    #run_rtf_parser(project_file_path=pu_raw_data)
    #run_docx_parser(project_file_path=raw_data)
    #run_doc_parser(project_file_path=doc_to_csv_path)
    run_pdf_parser(project_file_path=raw_data)

if __name__ == '__main__':
    main()