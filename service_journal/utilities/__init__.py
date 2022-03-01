__all__ = ['debug', 'utils']

try:
    from . import debug
    from .utils import *
except ImportError:
    from service_journal.utilities import debug
    from service_journal.utilities.utils import *
