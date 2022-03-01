# Dynamic local imports
try:
    from . import journal, exceptions, processors
except ImportError:
    from service_journal.classifications import journal, exceptions, processors

__all__ = ['journal', 'exceptions', 'processors']
