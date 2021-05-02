from typing import Dict, Optional, Tuple, Mapping, Set, Union, Iterable, Any
from numbers import Number
import pyodbc
from enum import Enum
from datetime import date

from service_journal.gen_utils.debug import get_default_logger
from service_journal.gen_utils.class_utils import pull_out_name, write_ordering, unpack, interpret_linestring

from pyodbc import ProgrammingError

logger = get_default_logger(__name__)
_to_date_format = '%Y-%m-%d'

# TODO: Add a schema file that defines all the keys for each data source.


def _package_rtbd(data, acc, to_date_format=_to_date_format):
    if data['route'] not in acc:
        acc[data['route']] = {}
    route = acc[data['route']]
    if data['trip_number'] not in route:
        route[data['trip_number']] = {}
    trip = route[data['trip_number']]
    if data['block_number'] not in trip:
        trip[data['block_number']] = {}
    block = trip[data['block_number']]
    # Convert date format
    date_key = data['date'].strftime(to_date_format)
    if date_key not in block:
        block[date_key] = []
    stops = block[date_key]
    stops.append({
        'time': data['trigger_time'],
        'lat': data['latitude'],
        'lon': data['longitude'],
        'dir': data['direction'],
        'bus': data['bus'],
        'operator': data['operator'],
        'depart': data['actual_time'],
        'boards': data['boards'],
        'alights': data['alights'],
        'onboard': data['onboard'],
        'date_time': data['trigger_time'],
        'stop_id': data['stop'],
        # TODO: Currently no SQL record for 'day', ask Tom
        'day': None
    })


def _package_dbt_schedule(data, acc, to_date_format=_to_date_format):
    date_key = data['date'].strftime(to_date_format)
    if date_key not in acc:
        acc[date_key] = {}
    date_value = acc[date_key]
    if data['block_number'] not in date_value:
        date_value[data['block_number']] = {}
    block = date_value[data['block_number']]
    if data['trip_number'] not in block:
        block[data['trip_number']] = {
            'route': data['route'],
            # TODO: Check with Tom that a trip can only be one route.
            'stops': {},
            'seq_tracker': 0,
        }
    trip = block[data['trip_number']]
    if data['stop'] is None or data['stop'] == '0':
        logger.warning('Got a 0 or NULL stop id in the schedule!')
    if data['stop'] not in trip['stops']:
        trip['stops'][data['stop']] = {
            'sched_time': data['sched_time'],
            'direction': data['direction'],
            'seen': 0,
            'bus': None,
            'confidence_score': 0,
            'confidence_factors': [],
            'operator': None,
            'trigger_time': None,
            'boards': 0,
            'alights': 0,
            'onboard': 0,
            'name': None
            # TODO: Remove name from output
            # 'direction': data['direction'],
        }


def _package_dbt_actuals(data, acc, to_date_format=_to_date_format):
    date_key = data['date'].strftime(to_date_format)
    if date_key not in acc:
        acc[date_key] = {}
    date_value = acc[date_key]
    if data['bus'] not in date_value:
        date_value[data['bus']] = {}
    bus = date_value[data['bus']]
    if data['trigger_time'] in bus:
        # Mid-swapping routes
        # TODO: Idk if this is ok or not. Check with Tom later.
        trigger_time_v = bus[data['trigger_time']]
        trigger_time_v['route'].add(data['route'])
        trigger_time_v['boards'] += data['boards']
        trigger_time_v['alights'] += data['alights']
        trigger_time_v['onboard'] = max(trigger_time_v['onboard'], data['onboard'])
    else:
        # Define a report
        bus[data['trigger_time']] = {
            'lat': data['latitude'],
            'lon': data['longitude'],
            'dir': data['direction'],
            'operator': data['operator'],
            'depart': data['actual_time'],
            'boards': data['boards'],
            'alights': data['alights'],
            'onboard': data['onboard'],
            'stop_id': data['stop'],
            'name': data['name'],
            'block_number': data['block_number'],
            'route': {data['route']},
            'trip_number': data['trip_number'],
        }


def _package_stop_locations(data, acc):
    if data['stop'] in acc:
        logger.warning('Overriding stop_num %s, why is there a duplicate?\nOld: %s New: %s', data['stop_num'],
                       acc[data['stop_num']], (data['latitude'], data['longitude']))
    acc[data['stop']] = (data['latitude'], data['longitude'])


def _package_shapes(data, acc, shape_str=True):
    key = data['from_stop'], data['to_stop']
    if key in acc:
        logger.warning('Overriding (%s, %s)\'s shape file.')
    distance = data['distance_feet']
    if shape_str:
        path = interpret_linestring(data['shape_str'])
    else:
        path = data.get('shape', [])
    acc[key] = distance, path


def process_cursor(cursor, sql_attr_map, packager, name=None, **kwargs):
    if name:
        logger.info('Processing cursor for %s.', name)
    acc = {}
    row = cursor.fetchone()
    attr_col_names = [sql_attr_map[col[0]] for col in cursor.description]
    while row:
        data = dict(zip(attr_col_names, row))
        packager(data, acc, **kwargs)
        row = cursor.fetchone()
    if name:
        logger.info('Finished processing cursor for %s.', name)
    return acc


class DataFormat(Enum):
    # Date-Block/Bus-Trip
    DBT = 'DBT'
    # Route-Trip-Block-DateKey
    RTBD = 'RTBD'


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

    def __init__(self, config: Mapping = None, open_: bool = False):
        if config is not None:
            self.config = config
        else:
            from .environ import config
            self.config = config
        try:
            settings = self.settings = config['settings']
            self.driver = settings['driver']
            self.username = settings['username']
            self.password = settings['password']
            self.host = settings['host']
            self.port = settings['port']
            self.is_open = False
        except KeyError as e:
            print(f'Error: Key ({e.args[0]}) not found in config.json. If this is your first time '
                  'running this, please setup your config and re-run it.')
            raise e

        attr_sql_map = settings['attr_sql_map']
        # Invert attr_sql_map
        sql_attr_map: Mapping[str, Mapping[str, Mapping[str, Union[str, bool]]]] = {
            table_name: {attr_data['name']: {
                'name': attr_name, 'nullable': attr_data['nullable'], 'view': attr_data['view']
            }
                for attr_name, attr_data in table_data.items()
            } for table_name, table_data in attr_sql_map.items()
        }

        self.attr_sql_map = attr_sql_map
        self.sql_attr_map = sql_attr_map

        self.connections = {
            'actuals_conn': None,
            'scheduled_conn': None,
            'output_conn': None,
            'stop_locations_conn': None,
            'shapes_conn': None,
        }
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

    def _connect(self, table_config: str) -> pyodbc.Connection:
        """
        Opens a connection to the specified config path.

        PARAMETERS
        --------
        table_config
            String representation of the table to read (from the config)
        RETURNS
        --------
        pyodbc.Connection
            A connection to the given table.
        """
        database = self.config['settings']["queries"][table_config]["database"]
        logger.debug(f'Connecting to {database} on {self.host}:{self.port} using {self.driver} and the credentials user'
                     f':{self.username} pass:****, and the table_config: {table_config}')
        # noinspection PyArgumentList
        return pyodbc.connect(driver=self.driver, server=self.host + ('' if self.port is None else f',{self.port}'),
                              database=database, uid=self.username, pwd=self.password)

    def _exc_query(self, conn_name, query_name, params=None, type_='default'):
        logger.info('Executing query (%s) on connection (%s).', query_name, conn_name)
        params = [] if params is None else params
        queries = self.config['settings']['queries']
        cursor = self.connections[conn_name].cursor()
        attr_sql_map = pull_out_name(self.attr_sql_map[query_name])
        sql_attr_map = pull_out_name(self.sql_attr_map[query_name])
        # attr_sql_map and queries are from config, and therefore trusted
        query = queries[query_name][type_].format(**attr_sql_map, table_name=queries[query_name]['table_name'])
        try:
            cursor.execute(query, *params)
        except pyodbc.Error as exc:
            logger.error('Pyodbc error, see raised exception. Query being run:\n%s\nParams: %s', query, params)
            raise exc
        logger.info('Finished executing query (%s) on connection (%s).', query_name, conn_name)
        return (attr_sql_map, sql_attr_map), cursor

    def open(self):
        """
        Open all the connections to the tables for the config.
        """
        # views_tables = self.attr_sql_map['views_tables']
        logger.info('Opening connections.')
        self.connections = {
            'actuals_conn': self._connect('actuals'),
            'scheduled_conn': self._connect('scheduled'),
            'output_conn': self._connect('output'),
            'stop_locations_conn': self._connect('stop_locations'),
            'shapes_conn': self._connect('shapes')
        }
        self.is_open = True
        logger.info('Connections opened.')

    def load_stop_loc(self) -> Mapping[str, Tuple[Number, Number]]:
        """
        Load stop locations from database. These are the geo-cords.
        """
        (stop_attr_sql_map, stop_sql_attr_map), cursor = self._exc_query('stop_locations_conn', 'stop_locations')
        return process_cursor(cursor, stop_sql_attr_map, _package_stop_locations)

    def load_shapes(self) -> Mapping[Tuple[int, int], Tuple]:
        # Execute query
        (attr_sql_map, sql_attr_map), cursor = self._exc_query('shapes_conn', 'shapes')

        return process_cursor(cursor, sql_attr_map, _package_shapes)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def read(self, date_: date, format_: DataFormat = DataFormat.DBT) -> Union[Tuple[Mapping, Mapping], Mapping]:
        """
        Read from the Connection the given date_ and store in the given format_.

        PARAMETERS
        --------
        date_
            The date object that defines the date from which to pull the schedule and avl_data.
        format_
            The DataFormat which should be used to store the data pulled from the Connection.
        """
        # a_xxx refers to "actuals xxx" and s_xxx refers to "scheduled xxx"
        logger.info('Reading from connections.')

        # Init params
        params = [date_.strftime(_to_date_format)]

        # Execute queries
        (a_attr_sql_map, a_sql_attr_map), a_cursor = self._exc_query('actuals_conn', 'actuals', params=params)
        (s_attr_sql_map, s_sql_attr_map), s_cursor = self._exc_query('scheduled_conn', 'scheduled', params=params)

        # Format dictates dictionary structure to generate
        if format_ is DataFormat.RTBD:
            return process_cursor(a_cursor, a_sql_attr_map, _package_rtbd, 'avl_dict')
        elif format_ is DataFormat.DBT:
            return process_cursor(s_cursor, s_sql_attr_map, _package_dbt_schedule, 'scheduled'), process_cursor(
                a_cursor, a_sql_attr_map, _package_dbt_actuals, 'actuals')
        else:
            raise NotImplementedError(f'The given format, {format_}, is not yet supported.')

    def write(self, data_map: Mapping, autocommit: bool = False):
        """
        Writes the data from data_map to the table.

        PARAMETERS
        --------
        data_map
            A mapping of attribute names to the values to be written to the table.
        autocommit
            Tells the function whether to commit changes after completing execution.
        """
        final_params = unpack(write_ordering, data_map)
        self._exc_query('output_conn', 'output', params=final_params)
        if autocommit:
            self.commit()

    def write_many(self, data_maps: Iterable[Mapping], autocommit: bool = True):
        """
        Writes a list of record mappings to the table.

        PARAMETERS
        --------
        data_maps
            The mappings to write to the table.
        autocommit
            If true, commits after writing all the changes. True by default.
        """
        logger.info('Writing many changes to output database.')
        for data_map in data_maps:
            self.write(data_map)
        if autocommit:
            self.commit()
        logger.info('Finished writing many changes to output database.')

    def commit(self):
        """
        Commits changes to the output database.
        """
        logger.debug('Committing written changes.')
        self.connections['output_conn'].commit()
        logger.debug('Changes committed.')
