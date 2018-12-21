

def build_directory(file_path, archive_name, extension):
    """
    Build a directory that will be
    sent to a remote server for processing.
    :return:
    """
    archive = zipfile.ZipFile(archive_name, 'w')

    if os.path.exists(file_path):
        os.chdir(file_path)
        print("Current Directory: {}".format(file_path))
        file_counter = 0
        files = glob.glob('*.{}'.format(extension))
        if len(files) > 0:
            for f in files:
                try:
                    archive.write(os.path.join(os.getcwd(), f))
                    file_counter += 1
                except:
                    print("WriteError: error writing {} to archive".format(files))
                finally:
                    print("Number of files added to archive: {}".format(file_counter))
        else:
            return "No files of extension {} found".format(extension)
