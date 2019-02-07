import six
import warnings
from functools import wraps

PYTHON3_SKIP = 'Feature does not work in python 3, (refer to https://github.com/NVIDIA/aistore/blob/master/openapi/README.md#future)'

def bytestring(b):
# Function for wraping bytearray as a string in python 3
 if six.PY3:
     return b.decode('ISO-8859-1')
 else:
     return b

def skipPython3(f):
    @wraps(f)
    def skip_if_py3_(self):
        if six.PY3:
            return self.skipTest(PYTHON3_SKIP)
        else:
            return f(self)

    return skip_if_py3_ 

def surpressResourceWarning():
    if six.PY3:
        warnings.simplefilter("ignore", ResourceWarning)

class DictParser(dict):
    __getattr__= dict.__getitem__

    def __init__(self, d):
        if six.PY3:
            iter = d.items()
        else:
            iter = d.iteritems()
        self.update(**dict((k, self.parse(v))
                           for k, v in iter))

    @classmethod
    def parse(cls, v):
        if isinstance(v, dict):
            return cls(v)
        elif isinstance(v, list):
            return [cls.parse(i) for i in v]
        else:
            return v
