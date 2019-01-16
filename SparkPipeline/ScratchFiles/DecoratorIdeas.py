"""
SparkPipeline
=============
Module builds out the Spark data pipeline.


@decorator(x, y, z)
def func(a, b):
    pass
# - same as the following
func = decorator(x, y, z)(func)
"""

from functools import wraps
from pprint import pprint
import inspect

def print1():
    return "in print1 func"

def print2():
    return "in print2 func"

funcs = [print1, print2]

def run_app():
    output = []
    for f in funcs:
        def decorate():
            @wraps(f)
            def wrapper(*args, **kwargs):
                g = f(*args, **kwargs)
                return g
            return wrapper
        d = decorate()
        output.append(d())
    return output


class User:
    def __init__(self, f):
        self.f = f
        self.user_count = 0
        self.users = []

    def __call__(self, *args, **kwargs):
        """
        Accounting for additional users.
        """
        self.user_count += 1
        new_user = self.f(*args, **kwargs)
        self.users.append(new_user)
        return new_user

    def get_user_count(self):
        return self.user_count

    def get_user_list(self):
        return self.users



@User
def AddUser(name):
    print("Adding {} to user list\n".format(name))
    return name


AddUser("William")
AddUser("Vamsi")
AddUser("Amy")

print AddUser.get_user_count()
