import sys
from typing import Generic as _Generic

if sys.version_info[:2] < (3, 7):
    # Py3.6 has a custom metaclass for Generic, which causes a lot of problems
    class SafeMeta(type):
        def __getitem__(self, item):
            return self


    class Generic(metaclass=SafeMeta):
        pass

else:
    SafeMeta = type
    Generic = _Generic
