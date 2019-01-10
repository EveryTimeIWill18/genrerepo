""
Credentials
===========
Credentials module safely loads login credentials.
"""

class Creds:
    """
    Creds
    -----
    Pass credentials to log into
    the Hadoop linux server.
    """


    def __init__(self):
        self.__creds     = None
        self.__username  = None
        self.__password  = None
        self.__server    = None

    @property
    def creds__(self):
        return self.creds__

    @creds__.setter
    def creds__(self, creds):
        self.creds__ = creds

    @creds__.deleter
    def creds__(self):
        del self.__creds

    @property
    def username__(self):
        return self.__username

    @username__.setter
    def username__(self, un):
        self.__username = un

    @property
    def password__(self):
        return self.__password

    @password__.setter
    def password__(self, pw):
        self.__password = pw

    @property
    def server__(self):
        return self.__server

    @server__.setter
    def server__(self, sr):
        self.__server = sr

    def get_creds(self):
        if self.creds__ is not None:
            try:
                with open(self.creds__, 'r') as credentials:
                    for i, line in enumerate(credentials):
                        if i == 0:
                            self.username__ = line
                        if i == 1:
                            self.password__ = line
                        if i == 2:
                            self.server__ = line
            except:
                pass
            finally:
                pass
