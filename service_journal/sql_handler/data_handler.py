from collections import OrderedDict
from typing import Dict, Optional, Tuple, Mapping, Set, Union, Iterable, List, MutableMapping, Callable, DefaultDict, Any
from numbers import Number
import logging
from abc import ABC, abstractmethod

from service_journal.utilities import pull_out_name, WRITE_ORDERING, unpack, deflt_dict, reorganize_write_fields
from service_journal.sql_handler import config as config_module

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
DATE_TIME_FORMAT = f'{DATE_FORMAT}_{TIME_FORMAT}'

logger = logging.getLogger(__name__)

# TODO: Integrate 

# Data chunks are sliced by day
class DataChunk:
    def __init__(self):
        self.representation = {}



class DataHandler(ABC):

    @abstractmethod
    def __getitem__(self, key):
        pass

    @abstractmethod
    def __setitem__(self, key, value):
        pass

class SQLHandler(DataHandler):
    
    def __init__(self, db):
        self.db = db

    def __getitem__(self, key):
        pass
# TODO: Move to an optional dependency with pandas
class LocalHandler(DataHandler):

    def __init__(self, file):
        pass
        # self.df = 


class Connection:
    stop_locations: Dict[str, Tuple[Number, Number]]
    config: Mapping
    driver: str
    username: str
    password: str
    host: str
    attr_sql_map: Mapping
    sql_attr_map: Mapping
    connections: Dict[str, Optional[pyodbc.Connection]]
    packagers: Mapping[str, Callable]

    def __init__(self, config: Mapping = None, open_: bool = False, packagers: Mapping[str, Callable] = None):
        self.is_open = False
        if config is not None:
            self.config = config
        else:
            config_module.setup()
            self.config = config_module.config
        try:
            settings = self.settings = self.config['settings']
            self.driver = settings['driver']
            self.username = settings['username']
            self.password = settings['password']
            self.host = settings['host']
            self.port = settings['port']
        except KeyError as e:
            print(f'Error: Key ({e.args[0]}) not found in config.json. If this is your first time '
                  'running this, please setup your config and re-run it.')
            raise e
        self.packagers = packagers if packagers is not None else {
            'stop_locations': _package_stop_locations,
            'shapes': _package_shapes,
            'actuals': _package_actuals,
            'scheduled': _package_scheduled,
        }
        self.connections = {}
        if open_:
            self.open()
        # Scope to fields for each view
        attr_sql_map = {
            type_: {
                view_name: view_data['fields'] for view_name, view_data in type_settings.items()
            } for type_, type_settings in self.config['attr_sql_map'].items()
        }

        # Invert attr_sql_map
        sql_attr_map: Mapping[str, Mapping[str, Mapping[str, Union[str, bool]]]] = {
            type_: {
                view_name: {
                    attr_data['name']: {
                            'name': attr_name, 'nullable': attr_data['nullable']
                    } for attr_name, attr_data in view_data.items()
                } for view_name, view_data in type_settings.items()
            } for type_, type_settings in attr_sql_map.items()
        }

        # Construct the connection lookup mappings
        self.connections = {
            type_: {
                view_name: None for view_name in type_settings.keys()
            } for type_, type_settings in attr_sql_map.items()
        }

        self.attr_sql_map = attr_sql_map
        self.sql_attr_map = sql_attr_map

        if open_:
            self.open()

    def close(self) -> Set[Exception]:
        """
        Closes all internal connections.

        RETURNS
        --------
        set
            Set of exceptions that occurred when closing all connections.
        """
        logger.info('Starting to close connections.')
        errors = set()

        # Attempt to close as many connections as possible, regardless of errors.
        def attempt_close(conn_name_, conn_):
            try:
                conn_.close()
            except AttributeError as e:
                logger.error('Ran into a AttributeError when closing %s. Is this connection an instance of '
                             'Connection?\n%s', conn_name_, e)
                errors.add(e)

        for conn_name, conn in self.connections.items():
            if conn is not None:
                attempt_close(conn_name, conn)

        if errors:
            logger.error('Ran into errors while closing connections. Errors:\n%s', '\n'.join(errors))
        logger.info('Connections closed.')
        self.is_open = False
        return errors

    def open(self):
        """
        Open all the connections to the tables for the config.
        """
        # views_tables = self.attr_sql_map['views_tables']
        logger.info('Opening connections.')
        for type_, views in self.connections.items():
            for view in views.keys():
                self.connections[type_][view] = self._connect(view)
        self.is_open = True
        logger.info('Connections opened.')

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def commit(self):
        """
        Commits changes to the output database.
        """
        logger.debug('Committing written changes.')
        self.connections['output_conn'].commit()
        logger.debug('Changes committed.')

    def get_deflt_loaders(self) -> Mapping[str, Callable]:
        """
        Returns a dictionary of default loaders.
        """
        return {
            'actuals': self.load_actuals,
            'scheduled': self.load_scheduled,
            'stop_locations': self.load_stop_locations,
            'shapes': self.load_shapes
        }