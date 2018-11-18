class DocToPdf(object):
    """Convert .doc files to .pdf"""
    def __init__(self, source_dir, destination_dir):
        self.source_dir = None
        self.destination_dir = destination_dir
        self.file_counter = 0
        self.current_file = None
        #self.logging_dir = logging_dir         # TODO
        #self.logging_config = dict()           # TODO
        self.conversion_lock = threading.Lock()
        try:
            if os.path.exists(source_dir):
                self.source_dir = source_dir
                os.chdir(self.source_dir)
            else:
                raise OSError("OSError: source directory:{} not found".format(source_dir))
        except OSError as e:
            print(e)
        finally:
            pass

    def start_windows_app(self, application, in_file, out_file, app_visible=False):
        """run windows application"""
        word = win32com.client.DispatchEx("{}".format(application))
        word.visible = app_visible
        doc = word.Documents.Open(in_file)
        print("doc name: {}".format(in_file))
        doc.SaveAs(out_file, FileFormat=17)
        doc.Close()


    def convert_to_pdf(self, output_dir):
        """convert files from .doc to .pdf"""
        start = time.time()
        try:
            if os.path.exists(output_dir):
                threads = []
                for files in os.listdir(os.getcwd()):
                    with self.conversion_lock:
                        if files.endswith("{}".format('.doc')):
                            try:
                                new_name = files.replace('.doc', 'r.pdf')
                                if new_name in os.listdir(os.getcwd()):
                                    print("{} already converted to .pdf".format(files))
                                else:
                                    print("Creating {}\nfrom: {}".format(new_name, files))
                                    in_file = os.path.abspath(os.getcwd() + '\\' + files)
                                    out_file = os.path.abspath(os.getcwd() + '\\' + new_name)
                                    # call star_windows_app in a thread
                                    app = 'Word.Application'
                                    #app_visible = True
                                    t = threading.Thread(target=self.start_windows_app, args=(app, in_file,
                                                                                              out_file))
                                    t.start()
                                    threads.append(t)
                                    # move the newly created pdf to a new directory
                                    destination_out_file = output_dir + '\\' + out_file
                                    shutil.move(out_file, destination_out_file)
                                    self.file_counter += 1
                            except:
                                print("Conversion Error! Could not convert: {} to pdf".format(files))
                for t in threads:
                    t.join()
            else:
                raise OSError("OSError: Output directory: {} could not be found.".format(output_dir))
        except OSError as e:
            print(e)
        finally:
            end = time.time()
            print("process took {} to run".format(end - start))
            print("{} files converted to .pdf".format(self.file_counter))

    def set_logging_config(self, level=logging.DEBUG, set_log_file=None,
                           make_logging_file=False, logfile_name=None, *format_args, **format_keys):
        """setup logging information for configuration.
        The logger uses dictionaries via DictConfig

        """
        if make_logging_file:
            if logfile_name is None:
                # create a default logfile name
                date = str(datetime.today())[:10]
                time = str(datetime.now())[11:]
                logfile_name = 'log_file_'+date+'_'+time
            else:
                date = str(datetime.today())[:10]
                time = str(datetime.now())[11:]
                logfile_name = logfile_name + date + '_' + time
        else:
            # must provide a log file name to find
            assert set_log_file is not None
            log_args = [a for a in format_args]
            log_keys = [k for k in format_keys]
            # create a dictionary from the logging keys,values
            log_dict = dict(zip(log_keys, log_args))
