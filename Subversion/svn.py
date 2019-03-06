"""
svn
~~~
This module controls the use of the subversion command
line interface tool. It can be used to automate working with
a subversion repository.
"""
import os
import sys
import io
import argparse
import subprocess
from functools import wraps, reduce, partial
from collections import OrderedDict



def svn_cli(username, password):
    """
    svn_cli decorator:
    kwargs: {'command name': 'arguments'}
    :param f:
    :return:
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            func = f(*args, **kwargs)
            counter = 0
            temp = [] # temporary arg container
            for k, v in func.items():
                if str(k).__contains__('svn'):
                    svnSplit = str(k).split(' ') # create a list
                    creds = '--username {} --password {}' \
                                .format(username, password)
                    svnSplit.insert(1, creds)
                    k = ' '.join(svnSplit)
                temp.append(' '.join([k, v]))
                counter += 1
                if counter < len(func):
                    temp.append('|')
            # - create a command line argument string
            cmd = ' '.join(t for t in temp)
            print cmd
            # - start the subprocess module
            p = subprocess.Popen(
                cmd, shell=True,
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE
            )
            output = list(p.communicate()) # run the command
            print output[0]
            return func
        return wrapper
    return decorator



@svn_cli(username='username', password='password')
def svn_list(svnDir, svnUrl):
    """Return a list of svn repositories."""
    svnDict = OrderedDict()
    if os.path.isdir(svnDir):
        svn_exe = svnDir
        svnDict['cd'] = svn_exe
    svnDict['svn list'] = svnUrl

    return svnDict



svn_list('C:\\Program Files\\TortoiseSVN\\bin',
         'http://ustrlxuda01.genre.com/svn/test2')
