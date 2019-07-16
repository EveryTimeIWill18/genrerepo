"""
check_parser_output
~~~~~~~~~~~~~~~~~~~~
This module is sued to test the accuracy of the file parsers.
"""
import os
import io
import csv
import sys
import subprocess
import socket
import PyPDF2




#raw_data = r'V:\Dev\Historical\20190521\Document'
# /genre/bda/apps/data_pipeline/python_scripts
pdf_name = r'V:\Dev\Historical\20190521\Document\Test3_0906ddd180d3a70e.pdf'
admin_dir = r'Y:\Shared\USD\Business Data and Analytics\Unstructured_Data_Pipeline\DMS_Claims\admin_log'
# raw_pdf = open(pdf_name, 'r').read()
# #memory_file = io.BytesIO(raw_pdf)
# print(raw_pdf)
# pdfReader = PyPDF2.PdfFileReader(open(pdf_name, 'rb'), strict=False)
# num_pages = pdfReader.getNumPages()
# print(pdfReader.isEncrypted)
#
# pg = 0
# while pg < num_pages:
#     current_page = pdfReader.getPage(pg)
#     #print(current_page .extractText())
#     pg += 1



with open(os.path.join(admin_dir,'names.csv'), 'w', newline='') as csvfile:
    fieldnames = ['first_name', 'last_name']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow({'first_name': 'Baked', 'last_name': 'Beans'})
    writer.writerow({'first_name': 'Lovely', 'last_name': 'Spam'})
    writer.writerow({'first_name': 'Wonderful', 'last_name': 'Spam'})