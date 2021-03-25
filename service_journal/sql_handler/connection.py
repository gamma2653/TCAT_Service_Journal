from typing import Dict, Optional, Tuple, Mapping, Set, Union
from numbers import Number
import pyodbc
from enum import Enum
from datetime import date

from service_journal.gen_utils.debug import get_default_logger
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
    dbt_sql_map: Mapping
    sql_dbt_map: Mapping
    connections: Dict[str, Optional[pyodbc.Connection]]

    def __init__(self, open_: bool = False, config: Mapping = None):
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
        except KeyError as e:
            print(f'Error: Key ({e.args[0]}) not found in config.json. If this is your first time '
                  'running this, please setup your config and re-run it.')
            raise e

        # setup dict key to sql field map and inverted map
        dbt_sql_map = settings['dbt_sql_map']
        sql_dbt_map = {}
        # TODO: Simplify
        for t in dbt_sql_map:
            if t not in sql_dbt_map:
                sql_dbt_map[t] = {}
            if t == 'actual' or t == 'scheduled':
                for field in dbt_sql_map[t]:
                    sql_dbt_map[t][dbt_sql_map[t][field]['name']] = {
                        'name': field,
                        'nullable': dbt_sql_map[t][field]['nullable'],
                        'view': dbt_sql_map[t][field]['view']
                    }
            else:
                sql_dbt_map[t] = dbt_sql_map[t]
        self.dbt_sql_map = dbt_sql_map
        self.sql_dbt_map = sql_dbt_map

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
        def attempt_close(conn_):
            try:
                conn_.close()
            except NameError as e:
                logger.error('Ran into a NameError when closing %s.\n%s', conn_, e)
                errors.add(e)
        for conn in self.connections.values():
            if conn is not None:
                attempt_close(conn)

        logger.info('Closed connections. Errors if any: %s', errors)
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
        return pyodbc.connect(rf'DRIVER={self.driver};'
                              rf'SERVER={self.host};'
                              rf'DATABASE={self.dbt_sql_map["views_tables"][table_config]["database"]};'
                              rf'UID={self.username};'
                              rf'PWD={self.password};')

    def open(self) -> None:
        """
        Open all the connections to the tables for the config.
        """
        # views_tables = self.dbt_sql_map['views_tables']
        self.connections = {
            'actual_read_conn': self._connect('actual'),
            'scheduled_read_conn': self._connect('scheduled'),
            'write_conn': self._connect('output'),
            'stop_locations_conn': self._connect('stop_locations')
        }

    def load_stop_loc(self) -> None:
        """
        Load stop locations from database. These are the geo-cords.
        """
        self.stop_locations = {}
        # grab query string
        query = self.dbt_sql_map['views_tables']['stop_locations']['static']
        # Grab cursor object
        cursor = self.connections['stop_locations_conn'].cursor()
        # Execute query
        cursor.execute(query)
        # Grab first row from result
        row = cursor.fetchone()

        # get dbt_names for each column for abstraction
        dbt_col_names = [self.sql_dbt_map['stop_locations'][col[0]]['name'] for col in cursor.description]
        while row:
            data = dict(zip(dbt_col_names, row))
            self.stop_locations[data['stop_num']] = (data['latitude'], data['longitude'])
            row = cursor.fetchone()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def read(self, date_: date, format_: DataFormat = DataFormat.RTBD) -> Union[Tuple[Mapping, Mapping], Mapping]:
        # grab queries
        a_query = self.dbt_sql_map['views_tables']['actual']['static']
        s_query = self.dbt_sql_map['views_tables']['scheduled']['static']
        # grab tables
        # a_table = self.dbt_sql_map['views_tables']['actual']['table']
        # sTable = self.dbt_sql_map['views_tables']['scheduled']['table']
        # grab cursors and execute queries
        a_cursor = self.connections['actual_read_conn'].cursor()
        a_cursor.execute(a_query, date_)
        s_cursor = self.connections['scheduled_read_conn'].cursor()
        s_cursor.execute(s_query, date)

        # (Not only for dbt format)
        a_sql_dbt_map, s_sql_dbt_map = self.dbt_sql_map['actual'], self.dbt_sql_map['scheduled']
        a_dbt_col_names = [a_sql_dbt_map[col[0]]['name'] for col in a_cursor.description]
        s_dbt_col_names = [s_sql_dbt_map[col[0]]['name'] for col in s_cursor.description]
        del a_sql_dbt_map, s_sql_dbt_map

        to_date_format = '%Y-%m-%d'
        # Format dictates dictionary structure to generate
        if format_ is DataFormat.RTBD:
            avl_dict = {}
            row = a_cursor.fetchone()
            while row:
                # standardizes references to columns so changes to database only
                # have to be changed in config.
                data = dict(zip(a_dbt_col_names, row))
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
            while row:
                data = dict(zip(s_dbt_col_names, row))
                date_key = data['date'].strftime(to_date_format)
                if data['date'] not in schedule:
                    schedule[date_key] = {}
                date_value = data[date_key]
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

            # Load in actuals
            avl_dict = {}
            row = a_cursor.fetchone()
            while row:

                # standardizes references to columns so changes to database only
                # have to be changed in config.
                data = dict(zip(a_dbt_col_names, row))
                date_key = data['date'].strftime(to_date_format)
                if data['date'] not in avl_dict:
                    avl_dict[date_key] = {}
                date_value = avl_dict[date_key]
                if data['bus'] not in date_value:
                    date_value[data['bus']] = {}
                bus = date_value[data['bus']]
                if data['trigger_time'] in bus:
                    print('Double time!\nFound another record at same time. Highly uncommon occurrence')
                    input('Press enter to continue.')
                # Definition of a report
                bus[data['trigger_time']] = {
                    'lat': data['latitude'],
                    'lon': data['longitude'],
                    'dir': data['direction'],
                    'bus': data['bus'],
                    'operator': data['operator'],
                    'depart': data['actual_time'],
                    'boards': data['boards'],
                    'alights': data['alights'],
                    'onboard': data['onboard'],
                    'stop_id': data['stop'],
                    'name': data['name'],
                    'block_number': data['block_number'],
                    'route': data['route']
                }
                del data
                row = a_cursor.fetchone()

            return schedule, avl_dict

    def write(self, data_map):
        cursor = self.connections['write_conn'].cursor()
        cursor.execute(self.dbt_sql_map['view_tables']['output']['static'])

    # data_to_write = [ for  ]
