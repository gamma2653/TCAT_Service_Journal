try:
    from . import connection, config
except ImportError:
    from service_journal.sql_handler import connection, config

__all__ = ['config', 'connection']