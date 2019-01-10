
"""
Tasks
=====
The Tasks module stores the different pipelines that will
be run in Main.

"""
from ProcessDocx.Networking.Connect import SftpConnection
from ProcessDocx.Networking.Credentials import Creds
from Pipeline.DataPipeline import Pipeline
from ProcessDocx.Networking.Connect import SftpConnection
from ProcessDocx.Networking.Credentials import Creds, creds_file
from SystemProcesses.SystemUtils import ConfigureFileDirectory
from tests.TestConnection import *
from tests.TestSftp import *

import os
import glob
from pprint import pprint


################################################################################
# SA Claims Tasks #
################################################################################
def load_docx():
    """
    Load .docx files into the Hadoop
    Linux server.
    """
    # - create an instance of the Pipeline class
    pipeline = Pipeline()

    # - path to write archive to
    dest = "C:\\Users\\WMurphy\\PycharmProjects" \
           "\\DataProcessingPipeline" \
           "\\ProcessDocx\\Networking"

    # - zipfile name
    DOCX_ZIP = 'ZIPPED_DOCX_FILES'

    @pipeline.add_task()
    def task_one():
        """
        Set the path directory.
        """
        return 'Z:\\WinRisk\\PC_BusinessAnalytics\\SA_Claims\\SA_2'

    @pipeline.add_task(dependent_nodes=task_one)
    def task_two(x):
        """
        Return the file count of .docx files.
        """
        os.chdir(x)
        return glob.glob('*.docx')

    @pipeline.add_task(dependent_nodes=task_one)
    def task_three(x):
        """
        Zip files up to prepare for pushing to Hadoop server.
        """
        # - build archive
        archive_created = False
        print("root dir: {}".format(x))
        try:
            #create_archive(x, dest, DOCX_ZIP)
            archive_created = True
            return archive_created
        except:
            print("ArchiveError: Could not create archive.")

    # - Run the pipeline
    output = pipeline.run()
    print(output)


################################################################################
# Personal Umbrella Tasks #
################################################################################

def personal_umbrella_load_docx():
    """
    Load .docx files to Hadoop linux server.
    """

    # - create an instance of the the Pipeline class
    pipeline = Pipeline()

    @pipeline.add_task()
    def task_one():
        """
        Set the path directory.
        """
        return "Z:\\WinRisk\\PC_BusinessAnalytics\\Claims Narratives"

    @pipeline.add_task(dependent_nodes=task_one)
    def task_two(x):
        """
        Return the file count of .docx files.
        """
        # - setup file configuration
        config = ConfigureFileDirectory(x, 'docx')
        child  = "PA_CLAIMS_DOCX"
        parent = "C:" \
                 "\\Users" \
                 "\\WMurphy\\" \
                 "PycharmProjects\\" \
                 "DataProcessingPipeline"

        # - build a temporary directory
        config.create_directory(child, parent)
        # - load in the docx files
        config.load_target_files()
        # - create the archive
        config.create_archive()

        # - pass archive location to next task
        return config.zipfile_name


    @pipeline.add_task(dependent_nodes=task_two)
    def task_three(x):
        """
        Push archive to remote hadoop env.
        """

        # - initiate the sftp connection
        connection = SftpConnection()

        # - setup credentials
        creds = Creds()
        creds.creds__ = creds_file
        connection.username__ = creds.username__
        connection.password__ = creds.password__
        connection.host__ = creds.server__
        connection.port__ = 22
        connection.local_path__ = x
        connection.remote_path__ = '/home/wmurphy'

        # - attempt sftp transfer
        connection.connect(filename=x)

        return "Finished Loading Docx Files"

    # - run the pipeline
    output = pipeline.run()
    pprint(output)


# - Test the personal umbrella claims pipeline
personal_umbrella_load_docx()
