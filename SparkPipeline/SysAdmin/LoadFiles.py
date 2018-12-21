import os
import glob
import shutil
import time
import threading


# --- lock for thread safety
lock = threading.Lock()


def load_external_data(project):
    """
    Function uses multiple threads to load
    in external data.
    Function copies the external data into
    the correct directory for use in the
    data pipelines.
    """
    lock.acquire()
    try:
        if project in list(FILE_EXTENSIONS.keys()):
            current_project = FILE_EXTENSIONS.get(project)

            # --- move to directory where raw files are stored
            os.chdir(list(SOURCE_PATHS.get(project).keys())[0])

            # --- load files into local directories
            for i, k in enumerate(list(current_project.keys())):
                file_counter = 0
                files = glob.glob('*{}'.format(k))
                current_extension_files = os.listdir(current_project.get(k))
                print('file type: {}'.format(k))
                print('Number of files: {}'.format(len(files)))
                for f in files:
                    if f not in current_extension_files:
                        try:
                            shutil.copy(f, current_project.get(k))
                            file_counter += 1
                        except:
                            print('Error copying {}'.format(f))
                print('Moved {} files\n---------\n {} files raised errors.'.format(file_counter,
                                                                                   len(files) - file_counter))
    except:
        print('Project Name: {} not found'.format(project))
    finally:
        lock.release()


def load_files(project, extension):
    """Load external files into local directories."""
    lock.acquire()
    try:
        if project in list(FILE_EXTENSIONS.keys()):
            current_project = FILE_EXTENSIONS.get(project)
            err_log = ERROR_FILES.get(project)

            # --- move to directory where raw files are stored
            os.chdir(list(SOURCE_PATHS.get(project).keys())[0])

            # --- list of file extensions
            file_extension_list = list(FILE_EXTENSIONS.get(project).keys())

            if extension in file_extension_list:
                # --- get the current path
                current_project_path = FILE_EXTENSIONS.get(project).get(extension)
                files = glob.glob('*{}'.format(extension))
                for f in files:
                    if f not in os.listdir(current_project_path):
                        try:
                            shutil.copy(f, current_project_path)
                            print('Successfully copied {}'.format(f))
                        except:
                            print('Error copying {}'.format(f))
                            with open(err_log, 'w') as err:
                                err.write(str(f))
    except:
        print('Project Name: {} not found'.format(project))
    finally:
        lock.release()


def run_job():
    """
    Run the load_external_data on multiple threads.
    """
    extensions = list(FILE_EXTENSIONS.get(CURRENT_PROJECTS[0]).keys())
    num_threads = len(extensions)

    threads = []
    start = time.time()
    for i in range(num_threads):
        thread = threading.Thread(target=load_files, args=(CURRENT_PROJECTS[0], extensions[i]))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    end = time.time()
    print("Run time: {}".format(end - start))





# --- execute run_job
run_job()


