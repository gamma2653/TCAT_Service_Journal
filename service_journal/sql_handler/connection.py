from typing import Dict, Optional, Tuple, Mapping, Set, Union
from numbers import Number
import pyodbc
from enum import Enum
from datetime import date

from service_journal.gen_utils.debug import get_default_logger

logger = get_default_logger(__name__)


class DataFormat(Enum):
    # Date-Block/Bus-Trip
    DBT: str = 'DBT'
    # Route-Trip-Block-DateKey
    RTBD: str = 'RTBD'


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

        # setup dict key to sql field map and inverted map
        attr_sql_map = settings['attr_sql_map']
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
            'actual_read_conn': None,
            'scheduled_read_conn': None,
            'write_conn': None,
            'stop_locations_conn': None
        }

        # Hacky way to add DataFormats as attributes to an instance.
        for item in DataFormat:
            setattr(self, item.name, item)
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

    def open(self) -> None:
        """
        Open all the connections to the tables for the config.
        """
        # views_tables = self.attr_sql_map['views_tables']
        logger.info('Opening connections.')
        self.connections = {
            'actual_read_conn': self._connect('actual'),
            'scheduled_read_conn': self._connect('scheduled'),
            'write_conn': self._connect('output'),
            'stop_locations_conn': self._connect('stop_locations')
        }
        logger.info('Connections opened.')

    def load_stop_loc(self) -> None:
        """
        Load stop locations from database. These are the geo-cords.
        """
        logger.info('Loading stop locations.')
        queries = self.config['settings']['queries']
        # grab query string
        query = queries['stop_locations']['static']
        self.stop_locations = {}
        # Grab cursor object
        cursor = self.connections['stop_locations_conn'].cursor()
        # Execute query
        cursor.execute(query)
        # Grab first row from result
        row = cursor.fetchone()

        # get attr_names for each column for abstraction
        attr_col_names = [self.sql_attr_map['stop_locations'][col[0]]['name'] for col in cursor.description]
        while row:
            data = dict(zip(attr_col_names, row))
            self.stop_locations[data['stop_num']] = (data['latitude'], data['longitude'])
            row = cursor.fetchone()
        logger.info('Stop locations loaded.')

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

        # Grab mappings for actuals and scheduled fields
        def pull_out_name(d):
            return {k: v['name'] for k, v in d.items()}

        a_sql_attr_map = pull_out_name(self.sql_attr_map['actual'])
        s_sql_attr_map = pull_out_name(self.sql_attr_map['scheduled'])
        a_attr_sql_map = pull_out_name(self.attr_sql_map['actual'])
        s_attr_sql_map = pull_out_name(self.attr_sql_map['scheduled'])

        # grab and format queries
        queries = self.config['settings']['queries']
        logger.debug('a_attr_sql_map: %s', a_attr_sql_map)
        a_query = queries['actual']['default'].format(**a_attr_sql_map, table_name=queries['actual']['table'])
        s_query = queries['scheduled']['default'].format(**s_attr_sql_map, table_name=queries['scheduled']['table'])

        # grab cursors and execute queries
        a_cursor = self.connections['actual_read_conn'].cursor()
        s_cursor = self.connections['scheduled_read_conn'].cursor()

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
        elif format_ is DataFormat.DBT:
            # Load schedule
            schedule = {}
            row = s_cursor.fetchone()
            logger.debug('Loading schedule in %s', format_)
            logger.debug('s_attr_col_names: %s', s_attr_col_names)
            while row:
                data = dict(zip(s_attr_col_names, row))
                date_key = data['date'].strftime(to_date_format)
                if data['date'] not in schedule:
                    schedule[date_key] = {}
                date_value = schedule[date_key]
                if data['block_number'] not in date_value:
                    date_value['block_number'] = {}
                block = date_value['block_number']
                if data['trip_number'] not in block:
                    block['trip_number'] = {
                        'route': data['route'],
                        'stops': {},
                        'seq_tracker': 0,
                    }
                trip = block['trip_number']
                if data['stop'] is None or data['stop'] == '0':
                    print('Got a 0 or NULL stop id')
                if data['stop'] not in trip['stops']:
                    trip['stops'][data['stop']] = {
                        'sched_time': data['sched_time'],
                        'direction': data['direction'],
                        'seen': 0,
                        'bus': None,
                        'confidence_score': 0,
                        'confidence_factors': []
                        # 'direction': data['direction'],
                    }
                del data
                row = s_cursor.fetchone()

            # Load in actuals
            avl_dict = {}
            row = a_cursor.fetchone()
            logger.debug('Loading actuals in %(format)s', format=format_)
            while row:

                # standardizes references to columns so changes to database only
                # have to be changed in config.
                data = dict(zip(a_attr_col_names, row))
                date_key = data['date'].strftime(to_date_format)
                if data['date'] not in avl_dict:
                    avl_dict[date_key] = {}
                date_value = avl_dict[date_key]
                if data['bus'] not in date_value:
                    date_value[data['bus']] = {}
                bus = date_value[data['bus']]
                if data['trigger_time'] in bus:
                    logger.warning('The same trigger time (%(trigger_time)s) was found twice for the same bus.'
                                   'Highly uncommon occurrence', trigger_time=data['trigger_time'])
                # Definition of a report
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
                    'route': data['route'],
                    'trip_number': data['trip_number'],
                }
                del data
                row = a_cursor.fetchone()
            logger.info('Done reading from connection.')
            return schedule, avl_dict
        else:
            return

    def write(self, data_map):
        logger.info('Starting to write to connection source.')
        queries = self.config['settings']['queries']
        cursor = self.connections['write_conn'].cursor()
        output_map = {k: v['name'] for k, v in self.attr_sql_map['output'].items()}
        cursor.execute(queries['output']['static'].format(**output_map))
        logger.info('Finished writing to connection source.')

    # data_to_write = [ for  ]
