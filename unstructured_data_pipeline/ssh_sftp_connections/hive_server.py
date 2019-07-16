"""
hive_server
~~~~~~~~~~~
"""
import os
import socks
import socket
import paramiko
from configparser import ConfigParser

from unstructured_data_pipeline.config_path import config_file_path
from unstructured_data_pipeline.logging import BaseLogger
from unstructured_data_pipeline.data_mining.data_parsers import d

# setup logging configuration
config = ConfigParser()
config.read(config_file_path)
sections = config.sections()
logfile = "dms_unst_pipeline_log_" + d
logger = BaseLogger(config.get(sections[2], 'DMS_LOGGING'), logfile)
logger.config() # use the default logging configuration


class BaseConnection(object):
    """Creates a connection to a remote server"""

    def __init__(self):
        self.is_connected: bool = False

    def __get_connection_credentials(self) -> tuple:
        """
        :private method:
        establish connection to the remote server
        """
        config = ConfigParser()
        config.read(config_file_path)
        sections = config.sections()
        host = config.get(sections[0], 'HOST')
        port = int(config.get(sections[0], 'PORT'))
        username = config.get(sections[0], 'USERNAME')
        password = config.get(sections[0], 'PASSWORD')
        return (host, port, username, password)

    def get_connection_credentials(self) -> tuple:
        """
        :public method:
        :return:
            :type tuple
        """
        return self.__get_connection_credentials()


class SftpConnection(BaseConnection):
    def __init__(self):
        super().__init__()
        self.sock: socks.socksocket = socks.socksocket()
        self.sftp: paramiko.Transport = None
        self.client: paramiko.SFTPClient.from_transport = None
        self.is_connected: bool = False
        self.sftp_connected: bool = False
        self.client_connected: bool = False

        logger.info(info=f"Beginning file transfer to linux server ...")

    def connect(self, filename: str, filepath: str, remote_path: str):
        """connect to the remote server"""
        try:
            host, port, username, password = self.get_connection_credentials()
            # set proxy connection values
            self.sock.set_proxy(
                proxy_type=None,
                addr=host,
                port=port,
                username=username,
                password=password
            )
            self.sock.connect((host, port))
            if socket.gethostname():
                self.is_connected = True
                print("Connection Successful:")
                print(f"HOST: {socket.gethostname()}")
                print(f"HOST FULL QUALIFIED NAME: {socket.getfqdn()}")
                print(f"HOST ADDRESS LIST: {socket.gethostbyaddr(host)}")
                # create transport
                self.sftp = paramiko.Transport(self.sock)
                try:
                    # connect sftp transport
                    self.sftp.connect(
                        username=username,
                        password=password
                    )
                    # check if connection is live
                    if self.sftp.is_alive():
                        print("Transport is live")
                        self.sftp_connected = True
                        # create client and connect
                        try:
                            # create a client
                            self.client = paramiko.SFTPClient.from_transport(self.sftp)
                            print(f"Client is: {self.client}")
                            self.client_connected = True
                            os.chdir(filepath)
                            self.transport_payload(
                                filename=filename,
                                filepath=filepath,
                                remote_path=remote_path
                            )
                            # close all open connections
                            self.client.close()
                            print("closing client")

                            self.sftp.close()
                            print("closing sftp")

                            self.sock.close()
                            print("closing socket")
                        except Exception as e:
                            logger.error(error=e)
                except Exception as e:
                    logger.error(error=e)
        except Exception as e:
            logger.error(error=e)
            logger.error(error='A connection error has occurred')


    def transport_payload(self, filename: str, filepath: str, remote_path: str):
        """Transport data to the remote server"""
        if self.sftp_connected and self.client_connected:
            try:
                while True:
                    os.chdir(filepath)
                    payload = filename
                    destination = remote_path + '/' + filename
                    self.client.put(
                        localpath=payload,
                        remotepath=destination
                    )
                    print(f"Successfully loaded {filename} to {destination}")
                    break
            except Exception as e:
                logger.error(error=e)
                logger.error(error="An error occurred while transporting payload")


class SSHConnection(object):
    def __init__(self):
        self.connected = False
        self.ssh = paramiko.SSHClient()

    def connect(self):
        """connect to a remote server via ssh"""
        config = ConfigParser()
        config.read(config_file_path)
        sections = config.sections()

        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.ssh.connect(
            hostname=config.get(sections[0], 'HOST'),
            username=config.get(sections[0], 'USERNAME'),
            password=config.get(sections[0], 'PASSWORD')
        )
        if socket.gethostname():
            self.connected = True
            print(socket.gethostname())
            print(socket.getfqdn())
            print(socket.gethostbyaddr(config.get(sections[0], 'HOST')))

            #cmd = "/genre/bda/apps/data_pipeline/python_scripts/server_script.sh;"
            # run the remote python script
            #stdout_ = self.ssh.exec_command('python ./pyspark_jobs.py')[1]
            # stdout = self.ssh.exec_command('python -c "exec(\\"' + open(r'C:\Users\wmurphy\PycharmProjects\server_side_pipeline_05_15_2019\pyspark_jobs.py'.__str__(),'r').read().strip('\n') + '\\".decode(\\"base64\\"))"' )[1]
            #
            # print('running')
            # stdout.channel.recv_exit_status()
            # lines = stdout.readlines()
            #
            # # check query output
            # for line in lines:
            #     print(line)
            # self.ssh.close()
            # Setup sftp connection and transmit this script
            #sftp = self.ssh.open_sftp()
            #sftp.put('C:\\Users\\wmurphy\\PycharmProjects\\server_side_pipeline_05_15_2019\\pyspark_jobs.py', 'myscript.py')
            #sftp.close()

            # Run the transmitted script remotely without args and show its output.
            # SSHClient.exec_command() returns the tuple (stdin,stdout,stderr)
            #stdin, stdout, stderr = self.ssh.exec_command('/genre/bda/apps/data_pipeline/python_scripts/server_script.sh')
            #print(stdout, stdin, stderr)
            #
            # for line in stdout:
            #     # Process each line in the remote output
            #     print(line)
            #
            # for line in stderr:
            #     print(line)


class RunRemoteScript(SSHConnection):
    def __init__(self, make_connection: bool):
        super().__init__()
        self.payload: str = None
        # connect to the remote server
        if make_connection:
            self.connect()
            print(f"Connected via ssh")

        logger.info(info=f"Beginning secure shell into the linux server ...")

    def execute_server_side_pipeline(self):
        """execute the server side portion of the data pipeline"""
        try:
            print("|||In execute_server_side_pipeline|||")
            cmd = "./server_script.sh"
            cmd = "(cd genre/bda/apps/data_pipeline/python_scripts/; ls -l;)"
            # run the remote python script
            stdin, stdout, stderr = self.ssh.exec_command('/genre/bda/apps/data_pipeline/python_scripts/server_script.sh')
            stdout.channel.recv_exit_status()
            lines = stdout.readlines()

            # check query output
            for line in lines:
                print(line)
        except Exception as e:
            logger.error(error=e)

    def execute_remote_commands(self, *args, **kwargs):
        """execute commands that will run in a remote server"""
        try:
            #cmd = 'beeline -u jdbc:hive2://mseaulxda01.genre.com:2181,mseaulxda02.genre.com:2181,mseaulxda03.genre.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2 -n WWMurphy -p Tr2oy2222222!'
            cmd = "hive -e 'use drw; show tables;'"
            # run the remote python script
            stdin_, stdout_, stderr_ = self.ssh.exec_command(cmd)
            stdout_.channel.recv_exit_status()
            lines = stdout_.readlines()

            # check query output
            for line in lines:
                print(line)
        except Exception as e:
            logger.error(error=e)

    def execute_hive_count(self, hive_table: str):
        """Execute a remote python script to get the Hive table count.
        """
        try:
            stdin, stdout, stderr = self.ssh.exec_command(f"/genre/bda/apps/data_pipeline/python_scripts/get_hive_size.sh '{hive_table}';")
            stdout.channel.recv_exit_status()
            lines = stdout.readlines()

            return lines[0].strip()
        except Exception as e:
            print(e)

def main():
    ssh = RunRemoteScript(make_connection=True)

    ssh.execute_server_side_pipeline()
    #hive_count = ssh.execute_hive_count(hive_table='emldata_2019_07_01')
    #print(hive_count)

if __name__ == '__main__':
    main()