class ProcessFiles:

    def __init__(self, file_path: str, file_ext: str) -> None:
        self.file_path: str = file_path
        self.file_ext: str = file_ext
        self.full_qualified_file: str = None
        self.files: list = []
        self.file_to_process: int = 0
        self.processed_files: int = 0
        self._processed_file_data: dict = {}

        if os.path.exists(self.file_path):
            # - list all the files with correct file extension and join with dir
            self.files = list(filter(lambda x: os.path.splitext(x)[1] == self.file_ext,
                                     list(map(lambda y: os.path.join(self.file_path, y),
                                          os.listdir(self.file_path)))))
            # - get the number of files with self.file_ext
            self.file_to_process = len(self.files)
        else:
            print("File: {} not found".format(self.file_path))

        #pprint.pprint(self.files)
       # pprint.pprint( self.file_to_process)

    def read_file_contents(self, chunk_size: int, num_files_to_process=1) -> None:
        """
        Read the contents of the current file.

        :return:
        """
        f_counter = 0
        while f_counter < num_files_to_process:
            with open(self.files[f_counter], 'r') as doc_file:
                content = doc_file.read()
                cleaned_content = content.replace('\n', '')
                cleaned_content = re.sub(r"\s+", " ", cleaned_content, flags=re.UNICODE)
                cleaned_content = re.sub(r"(\\x07|\\x0c|\\x15)","", cleaned_content, flags=re.UNICODE)
                cleaned_content = re.sub(r"(\r)","", cleaned_content, flags=re.UNICODE)
                cleaned_content = re.sub(r"\\r", "", cleaned_content)
                cleaned_content = re.sub(r"'", "", cleaned_content)

                key = cleaned_content.find('Claim Narrative.doc:')
                keysStart = [m.start() for m in re.finditer('(?=\d{0,10}\s\-\sClaim Narrative.doc)', cleaned_content)]
                #keysEnd = [m.start() for m in re.finditer('(?=.doc: )', cleaned_content)]
                print(keysStart)
                pprint.pprint(cleaned_content[keysStart[0]:keysStart[1]])






                return content
