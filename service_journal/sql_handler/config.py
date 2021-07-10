import os
import json
import sys


DEFAULT_CONFIG_NAME = 'config.json'


# TODO: Restructure config, combining queries and attr_sql_map.

DEFAULT_CONFIG = {
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
                'fields': {
                    'date': {
                        'name': 'service_day',
                        'nullable': True,
                    },
                    'block_number': {
                        'name': 'block',
                        'nullable': True,
                    },
                    'trip_number': {
                        'name': 'trip26',
                        'nullable': True,
                    },
                    'bus': {
                        'name': 'bus',
                        'nullable': True,
                    },
                    'trigger_time': {
                        'name': 'Time',
                        'nullable': True,
                    },
                    'operator': {
                        'name': 'Operator_Record_Id',
                        'nullable': True,
                    },
                    'actual_time': {
                        'name': 'Departure_Time',
                        'nullable': True,
                    },
                    'route': {
                        'name': 'Route',
                        'nullable': True,
                    },
                    'direction': {
                        'name': 'dir',
                        'nullable': True,
                    },
                    'stop': {
                        'name': 'Stop_Id',
                        'nullable': True,
                    },
                    'name': {
                        'name': 'Stop_Name',
                        'nullable': True,
                    },
                    'boards': {
                        'name': 'Boards',
                        'nullable': True,
                    },
                    'alights': {
                        'name': 'Alights',
                        'nullable': True,
                    },
                    'onboard': {
                        'name': 'Onboard',
                        'nullable': True,
                    },
                    'op_status': {
                        'name': 'OperationalStatus',
                        'nullable': True,
                    },
                    'latitude': {
                        'name': 'Latitude',
                        'nullable': True,
                    },
                    'longitude': {
                        'name': 'Longitude',
                        'nullable': True,
                    }
                },
                'query': {
                    'default': 'SELECT {date}, {block_number}, {trip_number}, {bus}, {trigger_time}, {operator}, '
                               '{actual_time}, {route}, {direction}, {stop}, {name}, {boards}, {alights}, {onboard}, '
                               '{op_status}, {latitude}, {longitude} FROM {table_name} WHERE {date}=? ORDER '
                               'BY {date}, {bus}, {trigger_time} asc',
                    'alternate': 'SELECT {date}, {block_number}, {trip_number}, {bus}, {trigger_time}, {operator}, '
                                 '{actual_time}, {route}, {direction}, {stop}, {name}, {boards}, {alights}, {onboard}, '
                                 '{op_status}, {latitude}, {longitude} FROM {table_name} WHERE {date}=? AND '
                                 '{block_number}=? ORDER BY {date}, {bus}, {trigger_time} asc',
                    'static': None,
                    'table_name': 'v_vehicle_history',
                    'database': 'TA_ITHACA_ACTUAL_HISTORY',
                }
            },
            'scheduled': {
                'fields': {
                    'date': {
                        'name': 'Service_Date',
                        'nullable': True,
                    },
                    'block_number': {
                        'name': 'block',
                        'nullable': True,
                    },
                    'trip_number': {
                        'name': 'trip26',
                        'nullable': True,
                    },
                    'route': {
                        'name': 'RouteNumber',
                        'nullable': True,
                    },
                    'direction': {
                        'name': 'Direction',
                        'nullable': True,
                    },
                    'stop': {
                        'name': 'stop_num',
                        'nullable': True,
                    },
                    'next_stop': {
                        'name': 'next_stop',
                        'nullable': True,
                    },
                    'sched_time': {
                        'name': 'departure_time',
                        'nullable': True,
                    }
                },
                'query': {
                    'default': 'SELECT {date}, {block_number}, {trip_number}, {route}, {direction}, {stop}, {next_stop}'
                               ', {sched_time} FROM {table_name} WHERE {date}=? ORDER BY {date}, {block_number}, '
                               '{trip_number}, {sched_time}',
                    'alternate': 'SELECT {date}, {block_number}, {trip_number}, {route}, {direction}, {stop}, '
                                 '{next_stop}, {sched_time} FROM {table_name} WHERE {date}=? AND {block_number}=? ORDER'
                                 ' BY {date}, {block_number}, {trip_number}, {sched_time}',
                    'static': None,
                    'table_name': 'v_scheduled_stops',
                    'database': 'TA_ITHACA_SCHEDULE_HISTORY',
                }
            },
            'output': {
                'fields': {
                    'date': {
                        'name': 'service_date',
                        'nullable': False,
                    },
                    'bus': {
                        'name': 'bus',
                        'nullable': True,
                    },
                    'report_time': {
                        'name': 'report_time',
                        'nullable': True,
                    },
                    'dir': {
                        'name': 'direction',
                        'nullable': True,
                    },
                    'route': {
                        'name': 'route',
                        'nullable': True,
                    },
                    'block_number': {
                        'name': 'block_number',
                        'nullable': False,
                    },
                    'trip_number': {
                        'name': 'trip_number',
                        'nullable': False,
                    },
                    'operator': {
                        'name': 'operator',
                        'nullable': True,
                    },
                    'boards': {
                        'name': 'boards',
                        'nullable': True,
                    },
                    'alights': {
                        'name': 'alights',
                        'nullable': True,
                    },
                    'onboard': {
                        'name': 'onboard',
                        'nullable': True,
                    },
                    'stop': {
                        'name': 'stop_id',
                        'nullable': False,
                    },
                    'stop_name': {
                        'name': 'stop_name',
                        'nullable': True,
                    },
                    'sched_time': {
                        'name': 'sched_time',
                        'nullable': True,
                    },
                    'seen': {
                        'name': 'seen',
                        'nullable': True,
                    },
                    'confidence_score': {
                        'name': 'confidence_score',
                        'nullable': True,
                    },
                },
                'query': {
                    'default': 'INSERT INTO {table_name} ({date}, {bus}, {report_time}, {dir}, {route}, {block_number},'
                               ' {trip_number}, {operator}, {boards}, {alights}, {onboard}, {stop}, {stop_name}, '
                               '{sched_time}, {seen}, {confidence_score}) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    'alternate': None,
                    'static': None,
                    'table_name': 'service_journal',
                    'database': 'segments',
                }
            },
            'stop_locations': {
                'fields': {
                    'stop': {
                        'name': 'stop_num',
                        'nullable': False,
                    },
                    'latitude': {
                        'name': 'latitude',
                        'nullable': False,
                    },
                    'longitude': {
                        'name': 'longitude',
                        'nullable': False,
                    }
                },
                'query': {
                    'default': 'SELECT {stop}, {latitude}, {longitude} FROM {table_name}',
                    'alternate': None,
                    'static': None,
                    'table_name': 'stops',
                    'database': 'Utilities',
                }
            },
            'shapes': {
                'fields': {
                    'from_stop': {
                        'name': 'fr_stop_num',
                        'nullable': False
                    },
                    'to_stop': {
                        'name': 'to_stop_num',
                        'nullable': False
                    },
                    'date_created': {
                        'name': 'ini_date',
                        'nullable': False
                    },
                    'distance_feet': {
                        'name': 'dist_ft',
                        'nullable': False
                    },
                    'shape': {
                        'name': 'seg_path',
                        'nullable': False
                    },
                    'shape_str': {
                        'name': 'seg_path_str',
                        'nullable': False
                    }
                },
                'query': {
                    'default': 'SELECT {from_stop}, {to_stop}, {date_created}, {distance_feet}, CAST({shape} AS '
                               'NVARCHAR(4000)) AS {shape_str} FROM {table_name} ORDER BY {date_created}',
                    'alternate': None,
                    'static': None,
                    'table_name': 'segment_dist',
                    'database': 'TA_ITHACA_SCHEDULE_HISTORY'
                }
            }
        }
    }
}


def init_config(default_config_):
    with open(DEFAULT_CONFIG_NAME, 'w') as f:
        json.dump(default_config_, f, indent=4)


def read_config(config_name_: str = DEFAULT_CONFIG_NAME):
    try:
        with open(config_name_, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        if os.environ.get('JOURNAL_USE_CONFIG_FILE', True):
            init_config(DEFAULT_CONFIG)
            print(f'Config initialized as {DEFAULT_CONFIG_NAME}. Please edit the {DEFAULT_CONFIG_NAME} with the '
                  f'appropriate information and restart.')
            sys.exit(1)
        else:
            return DEFAULT_CONFIG


config_name, config, settings, username, password, driver, host, port, attr_sql_map = (None, None, None, None, None,
                                                                                       None, None, None, None)


def setup():
    global config_name, config, settings, username, password, driver, host, port, attr_sql_map
    config_name = os.environ.get('JOURNAL_CONFIG_NAME', DEFAULT_CONFIG_NAME)
    config = read_config(config_name)
    settings = config['settings']

    # Set global var and config values to either the config value, or the environment variable if it exists.
    username = settings['username'] = os.environ.get('JOURNAL_SQL_USERNAME', settings['username'])
    password = settings['password'] = os.environ.get('JOURNAL_SQL_PASSWORD', settings['password'])
    driver = settings['driver'] = os.environ.get('JOURNAL_SQL_DRIVER', settings['driver'])
    host = settings['host'] = os.environ.get('JOURNAL_SQL_HOST', settings['host'])
    port = settings['port'] = os.environ.get('JOURNAL_SQL_PORT', settings['port'])

    attr_sql_map = settings['attr_sql_map']


if __name__ == '__main__':
    setup()
