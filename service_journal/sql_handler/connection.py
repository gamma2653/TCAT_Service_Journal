from collections import OrderedDict
from typing import Dict, Optional, Tuple, Mapping, Set, Union, Iterable, List, MutableMapping, Callable, DefaultDict
from numbers import Number
import pyodbc
from datetime import date

from shapely.geometry import LineString
from shapely.geometry.base import BaseGeometry
from shapely.wkt import loads as wkt_loads

from .query_builder import build_query
from ..utilities.debug import get_default_logger
from ..utilities.utils import pull_out_name, write_ordering, unpack, def_dict
from . import config as config_module

logger = get_default_logger(__name__)
DATE_FORMAT = '%Y-%m-%d'


# TODO: Add a schema file that defines all the keys for each data source.
def _package_schedule(data: Mapping, acc: DefaultDict, to_date_format: str = DATE_FORMAT):
    """
    Packages single entry of schedule data into date-block-trip data hierarchical format (acc).
    PARAMETERS
    --------
    data
        Single record mapping of data to insert into acc.
    acc
        The dictionary to insert data into.
    to_date_format
        The string pattern to format the date field to. See date.strftime() for more information. Default value is
        defined above.
    """
    try:
        date_key = data['date'].strftime(to_date_format)
    except AttributeError:
        if isinstance(data['date'], str):
            date_key = data['date']
        else:
            raise
    block = acc[date_key][data['block_number']]
    if data['trip_number'] not in block:
        block[data['trip_number']] = {
            'route': data['route'],
            # TODO: Check with Tom that a trip can only be one route.
            'stops': OrderedDict(),
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


def _package_actuals(data: Mapping, acc: DefaultDict, to_date_format: str = DATE_FORMAT):
    """
    Packages single entry of actual data into date-block-bus data hierarchical format (acc).
    PARAMETERS
    --------
    data
        Single record mapping of data to insert into acc.
    acc
        The dictionary to insert data into.
    to_date_format
        The string pattern to format the date field to. See date.strftime() for more information. Default value is
        defined above.
    """
    try:
        date_key = data['date'].strftime(to_date_format)
    except AttributeError:
        if isinstance(data['date'], str):
            date_key = data['date']
        else:
            raise
    bus = acc[date_key][data['bus']]
    if data['trigger_time'] in bus:
        # Mid-swapping routes
        # TODO: Idk if this is ok or not. Check with Tom later.
        trigger_time_v = bus[data['trigger_time']]
        trigger_time_v['route'].add(data['route'])
        if data['trip_number'] not in trigger_time_v['trip_number']:
            trigger_time_v['trip_number'].append(data['trip_number'])
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
            'trip_number': [data['trip_number']],
        }


def _package_stop_locations(data: Mapping, acc: MutableMapping):
    """
    Packages single entry of stop_location data into a hierarchical format (acc).
    PARAMETERS
    --------
    data
        Single record mapping of data to insert into acc.
    acc
        The dictionary to insert data into.
    """
    if data['stop'] in acc:
        logger.warning('Overriding stop_num %s, why is there a duplicate?\nOld: %s New: %s', data['stop_num'],
                       acc[data['stop_num']], (data['latitude'], data['longitude']))
    acc[data['stop']] = (data['latitude'], data['longitude'])


def _package_shapes(data: Mapping, acc: MutableMapping[Tuple[int, int], Tuple[float, BaseGeometry]],
                    shape_str: bool = True):
    """
    Packages single entry of shape data into a hierarchical format (acc).
    PARAMETERS
    --------
    data
        Single record mapping of data to insert into acc.
    acc
        The dictionary to insert data into.
    shape_str
        Whether the shape is stored in shape_str or shape. The specifics of what this means is somewhat lost on me. I
        believe the reason a difference exists is because pyodbc would sometimes store it one way, or another depending
        whether there was a shape or not; hence the default parameter in the case when shape_str is false and the
        interpret_linestring for when shape_str is true.
    """
    key = data['from_stop'], data['to_stop']
    if key in acc:
        logger.warning('Overriding (%s, %s)\'s shape file.')
    distance = data['distance_feet']
    if shape_str:
        try:
            path = wkt_loads(data['shape_str'])
        except TypeError:
            print(f'shape_str = {data["shape_str"]}')
            print(f'[fr: {data["from_stop"]}] [to: {data["to_stop"]}] [shape: {data.get("shape")}]')
            raise
    else:
        path = data.get('shape', LineString())
    acc[key] = distance, path


def process_cursor(cursor: pyodbc.Cursor, sql_attr_map: Mapping, packager: Callable, name=None, **kwargs):
    """
    Uses the cursor and packager to load the data and returns it.
    PARAMETERS
    --------
    cursor
        A pyodbc cursor which has already executed a query and is pointing to the start of the data you wish to read
        data from.
    sql_attr_map
        One of the parameters returned by Connection._exc_query(), used to bind SQL data to a dictionary.
    packager
        One of the packagers which takes a single record from the SQL table, and inserts it into a dictionary.
    name
        Used for the logger, just the option to have a name associated to this packaging process.
    """
    if name:
        logger.info('Processing cursor for %s.', name)
    acc = def_dict()
    row = cursor.fetchone()
    attr_col_names = [sql_attr_map[col[0]] for col in cursor.description]
    record_count = 0
    while row:
        data = dict(zip(attr_col_names, row))
        packager(data, acc, **kwargs)
        row = cursor.fetchone()
        record_count += 1
    if name:
        logger.info('Finished processing cursor for %s. Processed %s records.', name, record_count)
    return acc


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
            config_module.setup()
            self.config = config_module.config
        try:
            settings = self.settings = self.config['settings']
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

        # Scope to fields for each view
        attr_sql_map = {view_name: view_data['fields'] for view_name, view_data in settings['attr_sql_map'].items()}
        # Invert attr_sql_map
        sql_attr_map: Mapping[str, Mapping[str, Mapping[str, Union[str, bool]]]] = {
            view_name: {
                attr_data['name']: {
                        'name': attr_name, 'nullable': attr_data['nullable']
                } for attr_name, attr_data in view_data.items()
            } for view_name, view_data in attr_sql_map.items()
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

    def _connect(self, view_config: str) -> pyodbc.Connection:
        """
        Opens a connection to the specified config path.

        PARAMETERS
        --------
        view_config
            String representation of the table to read (from the config)
        RETURNS
        --------
        pyodbc.Connection
            A connection to the given table.
        """
        conn_config = self.config['settings']['attr_sql_map'][view_config]
        database = conn_config['database']

        # Get and remove Nones from additional args. @see pyodbc.connect for additional parameters
        additional_args_ = {'Trusted_connection': conn_config.get('trusted_connection')}
        additional_args = {}
        for key, value in additional_args_.items():
            if value is not None:
                additional_args[key] = value

        logger.debug(f'Connecting to {database} on {self.host}:{self.port} using {self.driver} and the credentials user'
                     f':{self.username} pass:****, and the table_config: {view_config}')

        try:
        # noinspection PyArgumentList
            return pyodbc.connect(driver=self.driver, server=self.host + ('' if self.port is None else f',{self.port}'),
                                  database=database, uid=self.username, pwd=self.password, **additional_args)
        except pyodbc.InterfaceError:
            logger.error(f'Failed to run query. These are the arguments:\ndriver:{self.driver},server:{self.host},'
                         f'port:{self.port},database:{database},user:{self.username},pwd:{self.password},'
                         f'{", ".join(map(str, additional_args.items()))}')
            raise

    def _exc_query(self, conn_name, query_name, params=None, which_query='default'):
        """
        Executes a single query defined in the config.

        PARAMETERS
        --------
        conn_name
            The name of the connection as defined in Connection.
        query_name
            The name of the query in the config.
        params
            The parameters to be passed to the SQL statement.
        which_query
            The type of query as defined in the config.
        """
        logger.info('Executing query (%s:%s) on connection (%s).', query_name, which_query, conn_name)
        params = [] if params is None else params
        query_config = self.config['settings']['attr_sql_map'][query_name]
        cursor = self.connections[conn_name].cursor()
        attr_sql_map = pull_out_name(self.attr_sql_map[query_name])
        sql_attr_map = pull_out_name(self.sql_attr_map[query_name])
        # attr_sql_map and queries are from config, and therefore trusted
        try:
            query_type = query_config['type'].upper()
        except KeyError:
            logger.error('While executing query %s, could not identify the query type in query config (%s)', query_name,
                         query_config)
            raise
        # Cast as set to remove duplicates, and for difference function
        fields = set(attr_sql_map.values())
        table_name = query_config['table_name']
        # Get SQL name for each attr in filters and order_by. Cast to list (order matters)
        filters = list(map(lambda x: attr_sql_map[x], query_config['filters'][which_query])) if 'filters' in \
                                                                                                query_config else None
        ordering = list(map(lambda x: attr_sql_map[x], query_config['order_by'])) if 'order_by' in \
                                                                                     query_config else None
        # Remove any fields to not be included in query
        not_included = filter(lambda x: query_config['fields'][x].get('do_not_include'), set(attr_sql_map.keys()))
        fields = list(fields.difference(not_included))

        special_fields = query_config.get('special_fields')
        query = build_query(query_type, fields, table_name, filters, ordering, special_fields)
        logger.info('Executing query:\n%s', query)
        try:
            cursor.execute(query, *params)
        except pyodbc.Error:
            logger.error('Pyodbc error, see raised exception. Query being run:\n%s\nParams: %s', query, params)
            raise
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
        """
        Load shapes from database. These are the coordinated of every vertices on the path between every stop.
        """
        # Execute query
        (attr_sql_map, sql_attr_map), cursor = self._exc_query('shapes_conn', 'shapes')

        return process_cursor(cursor, sql_attr_map, _package_shapes)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def read(self, date_: date, type_: str = 'default',
             params: Optional[List] = None) -> Tuple[Mapping, Mapping]:
        """
        Read from the Connection the given date_ and store in the given format_.

        PARAMETERS
        --------
        date_
            The date object that defines the date from which to pull the schedule and avl_data.
        type_
            The type of query being done
        params
            Parameters to pass to executed query.
        """
        # a_xxx refers to "actuals xxx" and s_xxx refers to "scheduled xxx"
        logger.info('Reading from connections.')
        # Init params
        if params is None:
            params = []
        params.insert(0, date_.strftime(DATE_FORMAT))

        # Execute queries
        (a_attr_sql_map, a_sql_attr_map), a_cursor = self._exc_query('actuals_conn', 'actuals', params=params,
                                                                     which_query=type_)
        (s_attr_sql_map, s_sql_attr_map), s_cursor = self._exc_query('scheduled_conn', 'scheduled', params=params,
                                                                     which_query=type_)

        # Format dictates dictionary structure to generate
        return process_cursor(s_cursor, s_sql_attr_map, _package_schedule, 'scheduled'), process_cursor(
                a_cursor, a_sql_attr_map, _package_actuals, 'actuals')

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
