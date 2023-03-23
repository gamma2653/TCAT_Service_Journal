__all__ = ['debug', 'utils']

try:
    from .utils import *
except ImportError:
    from utilities.utils import *
