# -*- coding: utf-8 -*-



class SftpCredentials:
    """
    SftpCredentials
    ==============
    Stores the sftp credentials that will be used
    to create an sftp connection.
    """

    def __init__(self):
        # private attributes
        # ------------------
        self.__username       = None
        self.__password       = None
        self.__host           = None
        self.__port           = None

    @property
    def username__(self):
        pass

    @username__.setter
    def username__(self, un):
        self.__username = un

    @username__.getter
    def username__(self):
        return self.__username

    @username__.deleter
    def username__(self):
        del self.__username

    @property
    def password__(self):
        pass

    @password__.setter
    def password__(self, pw):
        self.__password = pw

    @password__.getter
    def password__(self):
        return self.__password

    @password__.deleter
    def password__(self):
        del self.__password

    @property
    def host__(self):
        pass

    @host__.setter
    def host__(self, h):
        self.__host = h

    @host__.getter
    def host__(self):
        return self.__host

    @host__.deleter
    def host__(self):
        del self.__host

    @property
    def port__(self):
        pass

    @port__.setter
    def port__(self, p):
        self.__port = p

    @port__.getter
    def port__(self):
        return self.__port

    @port__.deleter
    def port__(self):
        del self.__port


class Data__:
    """
    Data__
    ============
    Data that will be passed to the remote server.
    """
    def __init__(self):
        # private attributes
        # ------------------
        self.__ext_type     = None
        self.__local_path   = None
        self.__remote_path  = None

    @property
    def local_path__(self):
        pass

    @local_path__.setter
    def local_path__(self, lp):
        self.__local_path = lp

    @local_path__.getter
    def local_path__(self):
        return self.__local_path

    @local_path__.deleter
    def local_path__(self):
        del self.__local_path

    @property
    def remote_path__(self):
        pass

    @remote_path__.setter
    def remote_path__(self, rp):
        self.__remote_path = rp

    @remote_path__.getter
    def remote_path__(self):
        return self.__remote_path

    @remote_path__.deleter
    def remote_path__(self):
        del self.__remote_path


class Archive:
    """
    Archive
    =======
    Build an archive of files that will be
    transferred to a remote server via sftp.
    """
    def __init__(self):
        self.__root_dir     = None
        self.__dest_dir     = None
        self.__archive_name = None

    @property
    def root_dir__(self):
        pass

    @root_dir__.setter
    def root_dir__(self, rd):
        self.__root_dir = rd

    @root_dir__.getter
    def root_dir__(self):
        return self.__root_dir

    @root_dir__.deleter
    def root_dir__(self):
        del self.__root_dir

    @property
    def dest_dir__(self):
        pass

    @dest_dir__.setter
    def dest_dir__(self, dd):
        self.__dest_dir = dd

    @dest_dir__.getter
    def dest_dir__(self):
        return self.__dest_dir

    @dest_dir__.deleter
    def dest_dir__(self):
        del self.__dest_dir

    @property
    def archive_name__(self):
        pass

    @archive_name__.setter
    def archive_name__(self, an):
        self.archive_name__ = an

    @archive_name__.getter
    def archive_name__(self):
        return self.__archive_name

    @archive_name__.deleter
    def archive_name__(self):
        del self.__archive_name
