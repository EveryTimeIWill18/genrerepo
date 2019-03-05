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
import subprocess
from functools import wraps, reduce, partial
from collections import OrderedDict


def svn_cli(f):
    """
    svn_cli decorator:
    kwargs: {'command name': 'arguments'}
    :param f:
    :return:
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        func = f(*args, **kwargs)
        counter = 0
        temp = [] # temporary arg container
        for k, v in func.items():
            temp.append(' '.join([k, v]))
            counter += 1
            if counter < len(func):
                temp.append('|')
        # - create a command line argument string
        cmd = ' '.join(t for t in temp)
        return func
    return wrapper


@svn_cli
def svn_list(svnDir, svnUrl):
    """Return a list of svn repositories."""
    svnDict = OrderedDict()
    if os.path.isdir(svnDir):
        svn_exe = os.path.join(svnDir, 'svn.exe')
        svnDict['cd'] = svn_exe
    svnDict['svn list'] = svnUrl

    return svnDict
