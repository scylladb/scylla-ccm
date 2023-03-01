
from .debug import *

import sys

#----------------------------------------------------------------------------------------------

class global_injector:
    def __init__(self):
        try:
            # Python 2
            self.__dict__['builtin'] = sys.modules['__builtin__'].__dict__
        except KeyError:
            # Python 3
            self.__dict__['builtin'] = sys.modules['builtins'].__dict__
    def __setattr__(self, name, value):
        self.builtin[name] = value

Global = global_injector()

#----------------------------------------------------------------------------------------------

Global.bb = bb
