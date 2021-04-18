import os
import json

default_config = {
    'settings': {
        # The host driver, as list of these can be found on the pyodbc library readme on github
        'driver': '{ODBC Driver 11 for SQL Server}',
        'host': 'AVAILDEV',
        'username': '',
        'password': '',
        'port': None,
        '_comment': 'Warning: Stored passwords are unencrypted. For testing purposes only',
        'attr_sql_map': {
            'actuals': {
                'date': {
                    'name': 'service_day',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'block_number': {
                    'name': 'block',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'trip_number': {
                    'name': 'trip26',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'bus': {
                    'name': 'bus',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'trigger_time': {
                    'name': 'Time',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'operator': {
                    'name': 'Operator_Record_Id',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'actual_time': {
                    'name': 'Departure_Time',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'route': {
                    'name': 'Route',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'direction': {
                    'name': 'dir',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'stop': {
                    'name': 'Stop_Id',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'name': {
                    'name': 'Stop_Name',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'boards': {
                    'name': 'Boards',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'alights': {
                    'name': 'Alights',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'onboard': {
                    'name': 'Onboard',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'op_status': {
                    'name': 'OperationalStatus',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'latitude': {
                    'name': 'Latitude',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'longitude': {
                    'name': 'Longitude',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                }
            },
            'scheduled': {
                'date': {
                    'name': 'Service_Date',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
                'block_number': {
                    'name': 'block',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
                'trip_number': {
                    'name': 'trip26',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
                'route': {
                    'name': 'RouteNumber',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
                'direction': {
                    'name': 'Direction',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
                'stop': {
                    'name': 'stop_num',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
                'next_stop': {
                    'name': 'next_stop',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
                'sched_time': {
                    'name': 'departure_time',
                    'view': 'v_scheduled_stops',
                    'nullable': True,
                },
            },
            'output': {
                'date': {
                    'name': 'service_date',
                    'view': '',
                    'nullable': False,
                },
                'bus': {
                    'name': 'bus',
                    'view': '',
                    'nullable': True,
                },
                'report_time': {
                    'name': 'report_time',
                    'view': '',
                    'nullable': True,
                },
                'dir': {
                    'name': 'direction',
                    'view': '',
                    'nullable': True,
                },
                'route': {
                    'name': 'route',
                    'view': '',
                    'nullable': True,
                },
                'block_number': {
                    'name': 'block_number',
                    'view': '',
                    'nullable': False,
                },
                'trip_number': {
                    'name': 'trip_number',
                    'view': '',
                    'nullable': False,
                },
                'operator': {
                    'name': 'operator',
                    'view': '',
                    'nullable': True,
                },
                'boards': {
                    'name': 'boards',
                    'view': '',
                    'nullable': True,
                },
                'alights': {
                    'name': 'alights',
                    'view': '',
                    'nullable': True,
                },
                'onboard': {
                    'name': 'onboard',
                    'view': '',
                    'nullable': True,
                },
                'stop': {
                    'name': 'stop_id',
                    'view': '',
                    'nullable': False,
                },
                'stop_name': {
                    'name': 'stop_name',
                    'view': '',
                    'nullable': True,
                },
                'sched_time': {
                    'name': 'sched_time',
                    'view': '',
                    'nullable': True,
                },
                'seen': {
                    'name': 'seen',
                    'view': '',
                    'nullable': True,
                },
                'confidence_score': {
                    'name': 'confidence_score',
                    'view': '',
                    'nullable': True,
                },
            },
            'stop_locations': {
                'stop': {
                    'name': 'stop_num',
                    'view': 'stop_locations',
                    'nullable': False,
                },
                'latitude': {
                    'name': 'latitude',
                    'view': 'stop_locations',
                    'nullable': False,
                },
                'longitude': {
                    'name': 'longitude',
                    'view': 'stop_locations',
                    'nullable': False,
                }
            }
        },
        'queries': {
            'actuals': {
                'default': 'SELECT {date}, {block_number}, {trip_number}, {bus}, {trigger_time}, {operator}, '
                           '{actual_time}, {route}, {direction}, {stop}, {name}, {boards}, {alights}, {onboard}, '
                           '{op_status}, {latitude}, {longitude} FROM {table_name} WHERE {date}=? ORDER '
                           'BY {date}, {bus}, {trigger_time} asc',
                'alternate': None,
                'static': None,
                'table_name': 'v_vehicle_history',
                'database': 'TA_ITHACA_ACTUAL_HISTORY',
            },
            'scheduled': {
                'default': 'SELECT {date}, {block_number}, {trip_number}, {route}, {direction}, {stop}, {next_stop}, '
                           '{sched_time} FROM {table_name} WHERE {date}=?',
                'alternate': None,
                'static': None,
                'table_name': 'v_scheduled_stops',
                'database': 'TA_ITHACA_SCHEDULE_HISTORY',
            },
            'output': {
                'default': 'INSERT INTO {table_name} ({date}, {bus}, {report_time}, {dir}, {route}, {block_number}, '
                           '{trip_number}, {operator}, {boards}, {alights}, {onboard}, {stop}, {stop_name}, '
                           '{sched_time}, {seen}, {confidence_score}) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                'alternate': None,
                'static': None,
                'table_name': 'service_journal',
                'database': 'segments',
            },
            'stop_locations': {
                'default': 'SELECT {stop}, {latitude}, {longitude} FROM {table_name}',
                'alternate': None,
                'static': None,
                'table_name': 'stops',
                'database': 'Utilities',
            }
        }
    }
}


def init_config():
    with open('config.json', 'w') as f:
        json.dump(default_config, f, sort_keys=True, indent=4)


def read_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        init_config()
        return default_config


config = read_config()
settings = config['settings']

username = settings['username'] = os.environ.get('SQL_USERNAME', settings['username'])
password = settings['password'] = os.environ.get('SQL_PASSWORD', settings['password'])
driver = settings['driver'] = os.environ.get('SQL_DRIVER', settings['driver'])
host = settings['host'] = os.environ.get('SQL_HOST', settings['host'])
port = settings['port'] = os.environ.get('SQL_PORT', settings['port'])
# I should simplify the format of dbt_sql_map to make inverting easier.
attr_sql_map = settings['attr_sql_map']
