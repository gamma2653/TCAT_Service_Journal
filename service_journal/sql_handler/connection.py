from typing import Dict, Optional, Tuple, Mapping, Set, Union, Iterable
from numbers import Number
import pyodbc
from enum import Enum
from datetime import date

from service_journal.gen_utils.debug import get_default_logger
from service_journal.gen_utils.class_utils import pull_out_name, write_ordering, unpack

from pyodbc import ProgrammingError

logger = get_default_logger(__name__)


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
        self.stop_locations = {}
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
            'stop_locations_conn': None
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
            'stop_locations_conn': self._connect('stop_locations')
        }
        logger.info('Connections opened.')

    def load_stop_loc(self):
        """
        Load stop locations from database. These are the geo-cords.
        """
        logger.info('Loading stop locations.')
        queries = self.config['settings']['queries']
        self.stop_locations = stop_locations = {}
        # Grab cursor object
        cursor = self.connections['stop_locations_conn'].cursor()
        stop_attr_sql_map = pull_out_name(self.attr_sql_map['stop_locations'])
        stop_sql_attr_map = pull_out_name(self.sql_attr_map['stop_locations'])
        # Execute query
        cursor.execute(queries['stop_locations']['default'].format(**stop_attr_sql_map,
                                                                   table_name=queries['stop_locations']['table_name']))
        # Grab first row from result
        row = cursor.fetchone()

        # get attr_names for each column for abstraction
        attr_col_names = [stop_sql_attr_map[col[0]] for col in cursor.description]
        while row:
            data = dict(zip(attr_col_names, row))
            if data['stop_num'] in stop_locations:
                logger.warning('Overriding stop_num %s, why is there a duplicate?\nOld: %s New: %s', data['stop_num'],
                               stop_locations[data['stop_num']], (data['latitude'], data['longitude']))
            stop_locations[data['stop_num']] = (data['latitude'], data['longitude'])
            row = cursor.fetchone()
        logger.info('Stop locations loaded.')

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    @staticmethod
    def _read_rtbd(a_cursor: pyodbc.Cursor, a_attr_col_names: Iterable[str], format_: DataFormat,
                   to_date_format: str) -> Mapping:
        avl_dict = {}
        row = a_cursor.fetchone()
        logger.debug('Loading actuals in %(format)s', format=format_)
        while row:
            # standardizes references to columns so changes to database only
            # have to be changed in config.
            data = dict(zip(a_attr_col_names, row))
            if data['route'] not in avl_dict:
                avl_dict[data['route']] = {}
            route = avl_dict[data['route']]
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
            del data
            row = a_cursor.fetchone()
        return avl_dict

    @staticmethod
    def _read_dbt(cursors: Tuple[pyodbc.Cursor, pyodbc.Cursor], col_names: Tuple[Iterable[str], Iterable[str]],
                  format_: DataFormat, to_date_format: str) -> Tuple[Mapping, Mapping]:
        a_cursor, s_cursor = cursors
        a_attr_col_names, s_attr_col_names = col_names
        logger.info('Loading schedule in %s', format_)
        logger.debug('s_attr_col_names: %s', s_attr_col_names)

        schedule = {}
        row = s_cursor.fetchone()
        while row:
            # standardizes references to columns so changes to database only
            # have to be changed in config.
            data = dict(zip(s_attr_col_names, row))
            date_key = data['date'].strftime(to_date_format)
            if date_key not in schedule:
                schedule[date_key] = {}
            date_value = schedule[date_key]
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
            del data
            row = s_cursor.fetchone()

        logger.debug('Loading actuals in %s', format_)
        avl_dict = {}
        row = a_cursor.fetchone()
        while row:
            data = dict(zip(a_attr_col_names, row))
            date_key = data['date'].strftime(to_date_format)
            if date_key not in avl_dict:
                avl_dict[date_key] = {}
            date_value = avl_dict[date_key]
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
            del data
            row = a_cursor.fetchone()
        logger.info('Done reading from connection.')
        logger.debug(
            'Returning values with these keys.\nschedule=%s\navl_dict=%s', schedule.keys(), avl_dict.keys())
        return schedule, avl_dict

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

        # Grab mappings for actuals and scheduled fields
        a_sql_attr_map = pull_out_name(self.sql_attr_map['actuals'])
        s_sql_attr_map = pull_out_name(self.sql_attr_map['scheduled'])
        a_attr_sql_map = pull_out_name(self.attr_sql_map['actuals'])
        s_attr_sql_map = pull_out_name(self.attr_sql_map['scheduled'])

        # grab and format queries
        queries = self.config['settings']['queries']
        logger.debug('a_attr_sql_map: %s', a_attr_sql_map)
        a_query = queries['actuals']['default'].format(**a_attr_sql_map, table_name=queries['actuals']['table_name'])
        s_query = queries['scheduled']['default'].format(
            **s_attr_sql_map, table_name=queries['scheduled']['table_name'])

        # grab cursors and execute queries
        a_cursor = self.connections['actuals_conn'].cursor()
        s_cursor = self.connections['scheduled_conn'].cursor()

        # Execute queries
        a_cursor.execute(a_query, date_)
        s_cursor.execute(s_query, date_)

        # Get column names based on query results
        # This protects against queries that do not have all the attributes, and makes packaging the data easier
        a_attr_col_names = [a_sql_attr_map[col[0]] for col in a_cursor.description]
        s_attr_col_names = [s_sql_attr_map[col[0]] for col in s_cursor.description]
        del a_attr_sql_map, s_attr_sql_map

        to_date_format = '%Y-%m-%d'

        # Format dictates dictionary structure to generate
        if format_ is DataFormat.RTBD:
            return self._read_rtbd(a_cursor, a_attr_col_names, format_, to_date_format)
        elif format_ is DataFormat.DBT:
            return self._read_dbt((a_cursor, s_cursor), (a_attr_col_names, s_attr_col_names), format_, to_date_format)
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
        logger.debug('Starting to write a record to connection source.')
        queries = self.config['settings']['queries']['output']
        query = queries['default']
        table_name = queries['table_name']
        cursor = self.connections['output_conn'].cursor()
        output_map = pull_out_name(self.attr_sql_map['output'])
        # TODO: Potential to get all above data from function call. Repeated in read.

        # Wish I could unpack using "*" the values of data_map, but order becomes an issue.
        # Solved by using write_ordering.
        # TODO: This is NOT DRY, refactor later.
        # format replaces attribute names with sql names, and unpack fills the ?'s with values.
        final_query = query.format(**output_map, table_name=table_name)
        final_params = unpack(write_ordering, data_map)
        try:
            cursor.execute(final_query, *final_params)
        except ProgrammingError as exc:
            logger.error('Failed to write.\nQuery:\n%s\nParams:\n%s', final_query, final_params)
            raise exc
        logger.debug('Finished writing a record to connection source.')
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
