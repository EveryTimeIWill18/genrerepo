"""
update_document_contents
~~~~~~~~~~~~~~~~~~~~~~~~
Update the document contents for previously loaded data sets.
"""
import os
import abc
import pickle
from pprint import pprint
from configparser import ConfigParser
from unstructured_data_pipeline.data_mining.data_parsers import d
from unstructured_data_pipeline.config_path import config_file_path
from unstructured_data_pipeline.data_mining.meta_data import MetaData, meta_data_log_dir

# setup logging configuration
config = ConfigParser()
config.read(config_file_path)
sections = config.sections()
pickle_path = config.get(sections[1], 'DMS_PICKLE')

metadata_xls_copy_path = r"V:\Dev\Delta\20190619\Metadata"
metadata_copy_file = r"20190619 - Copy.xls"

class UpdateMappingDictionaryInterface(metaclass=abc.ABCMeta):
    """Abstract Interface"""

    @abc.abstractmethod
    def load_pickled_file(self, file_path: str):
        """load in the pickled mapping dictionary"""
        raise NotImplementedError

    @abc.abstractmethod
    def update_mapping_dictionary(self):
        """Update the contents of the mapping dictionary
        that has the same filename as the new file's metadata.previous name
        """
        raise NotImplementedError

    @abc.abstractmethod
    def load_metadata_file(self, file_path: str, metadata_file: str):
        """Load the metadata file that contains the
        file's previous filename.
        """
        raise NotImplementedError


class ConcreteUpdateDictionary(UpdateMappingDictionaryInterface):
    """Concrete implementation of the UpdateMappingDictionaryInterface"""

    def __init__(self, file_type: str):
        self.file_type = file_type
        self.current_pkl_file: str = None
        self.pkl_dict: dict = None
        self.metadata_df: MetaData = None

    def load_pickled_file(self, file_path: str):
        try:
            if self.file_type != 'pdf':
                pkl_file = self.file_type.title() + 'ParserMappingDict_' + d + '.pickle'
                if pkl_file in os.listdir(file_path):
                    self.current_pkl_file = pkl_file
                    self.pkl_dict = pickle.load(open(os.path.join(file_path, self.current_pkl_file), 'rb'))
                    #print(f"pickle file keys: {self.pkl_dict.keys()}")
            elif self.file_type == 'pdf':
                # PdfParserMappingContainer_2019_07_24.pickle
                pkl_file = self.file_type.title() + 'ParserMappingContainer_' + d + '.pickle'
                if pkl_file in os.listdir(file_path):
                    self.current_pkl_file = pkl_file
                    self.pkl_dict = pickle.load(open(os.path.join(file_path, self.current_pkl_file), 'rb'))

        except Exception as e:
            print(e)

    def update_mapping_dictionary(self):
        try:
            pass
        except Exception as e:
            print(e)

    def load_metadata_file(self, file_path: str, metadata_file: str):
        try:
            if metadata_file in os.listdir(file_path):
                md = MetaData(os.path.join(meta_data_log_dir, 'metadata_log.txt'))
                self.metadata_df = md.load_metadata_as_df(full_file_path=os.path.join(file_path, metadata_file))
        except Exception as e:
            print(e)




def update_pdf_dict():
    pass

def update_eml_dict():
    eml_pkl_dict = ConcreteUpdateDictionary(file_type='eml')
    eml_pkl_dict.load_pickled_file(file_path=pickle_path)
    eml_pkl_dict.load_metadata_file(file_path=metadata_xls_copy_path, metadata_file=metadata_copy_file)

    eml_pickled_files = list(eml_pkl_dict.pkl_dict.keys())
    eml_prior_v_obj_name = list(eml_pkl_dict.metadata_df['Prior_Version_Object_Name'].dropna())
    eml_current_v_obj_name = list(eml_pkl_dict.metadata_df.Object_name[eml_pkl_dict.metadata_df['Prior_Version_Object_Name'] != 'NaN'])
    #pprint(eml_pickled_files)
    #pprint(eml_prior_v_obj_name)
    pprint(len(eml_current_v_obj_name))

def update_rtf_dict():
    pass

def update_doc_dict():
    pass

def update_docx_dict():
    pass


def main():
    update_eml_dict()
    # for v in pkl_dict.metadata_df['File_Name'].values():
    #     print(v)

if __name__ == '__main__':
    main()
