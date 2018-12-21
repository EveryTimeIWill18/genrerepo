import os
import stat
import glob
import time
import zipfile
import multiprocessing
from psutil import cpu_count, cpu_stats, cpu_times
from win32com import client
import comtypes.client


def convert_doc_to_pdf():
    """
    Convert ms word documents to pdf.
    :return: None
    """

    folder = "PATH TO WORD DOCUMENTS"
    os.chdir(folder)
    g = glob.glob('*.pdf')
    try:
        word = client.DispatchEx("Word.Application")
        word.Visible = False
        for files in os.listdir(os.getcwd()):
            try:
                if files.endswith(".doc"):
                    #file_attributes = os.stat(files)[0]
                    #if (not file_attributes & stat.S_IWRITE):
                    #    print("file: {} is read only")
                    #    os.chmod(files, stat.S_IWRITE)
                    new_name = files.replace(".doc", ".pdf")
                    if new_name in g:
                        pass
                        #print("{} already converted to pdf".format(new_name))
                    else:
                        in_file = os.path.abspath(folder + "\\" + files)
                        new_file = os.path.abspath(folder + "\\" + new_name)
                        doc = word.Documents.Open(in_file)
                        doc.SaveAs(new_file, FileFormat=17)
                        doc.Close()
                        print("saving {}".format(new_file))
            except:
                print("{} raised an error".format(files))
    except Exception, e:
        print e
    finally:
        word.Quit()
        
def main():
    # --- total number of cpus
    # num_cpus = multiprocessing.cpu_count()
    start = time.time()
    # --- setup for running 4 concurrent processes
    p1 = multiprocessing.Process(target=convert_doc_to_pdf)
    p2 = multiprocessing.Process(target=convert_doc_to_pdf)
    p3 = multiprocessing.Process(target=convert_doc_to_pdf)
    p4 = multiprocessing.Process(target=convert_doc_to_pdf)
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    for process in [p1, p2, p3, p4]:
        process.join()
    #doc_to_pdf()
    print('Total Run time: {}'.format(time.time() - start))
if __name__ == '__main__':
    main()
