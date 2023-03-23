try:
    from . import connection, config
except ImportError:
    from sql_handler import connection, config

__all__ = ['config', 'connection']