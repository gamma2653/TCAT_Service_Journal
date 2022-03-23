__all__ = ['debug', 'utils']

try:
    from .utils import *
except ImportError:
    from service_journal.utilities.utils import *
