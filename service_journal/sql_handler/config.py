import os
import json
import sys

try:
    from ..utilities.utils import ENVIRONMENT_TRUTHY_VALUES
except ImportError:
    from service_journal.utilities.utils import ENVIRONMENT_TRUTHY_VALUES


# ENVIRONMENT VARIABLES:
# JOURNAL_USE_CONFIG_FILE
#   Determines whether to use the config file or depends on defaults/environment variables. Default: true
# JOURNAL_FORCE_USE_CONFIG_FILE
#   Determines whether to force the usage of a config file. Overrides value of JOURNAL_USE_CONFIG_FILE. Default: true
# JOURNAL_CONFIG_NAME
#   The name of the config file to use. Default: DEFAULT_CONFIG_NAME (see below)
# JOURNAL_SQL_USERNAME
#   Overrides the config's SQL username
# JOURNAL_SQL_PASSWORD
#   Overrides the config's SQL password
# JOURNAL_SQL_DRIVER
#   Overrides the config's SQL driver
# JOURNAL_SQL_HOST
#   Overrides the config's SQL host server
# JOURNAL_SQL_PORT
#   Overrides the config's SQL port

# FIXME: Currently if config file exists, it overrides all values of default value, while environment vars only replace
# one value.
# Priority of config values from highest to lowest priority:
# 1. Environment vars
# 2. Config file value
# 3. Default value


DEFAULT_CONFIG_NAME = 'config.json'

# TODO: Restructure config, turning it into a ChainMap.

DEFAULT_CONFIG = {
    'settings': {
        # The host driver, as list of these can be found on the pyodbc library readme on github
        'driver': '{SQL SERVER}',
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
                'type': 'SELECT',
                'filters': {
                    'default': [
                        'date'
                    ],
                    'alternate': [
                        'date',
                        'block_number'
                    ]
                },
                'order_by': [
                    'date', 'bus', 'trigger_time'
                ],
                'table_name': 'v_vehicle_history',
                'database': 'TA_ITHACA_ACTUAL_HISTORY',
            },
            'scheduled': {
                'fields': {
                    'date': {
                        'name': 'Service_Date',
                        'nullable': True,
                    },
                    'block_number': {
                        'name': 'BlockNumber',
                        'nullable': True,
                    },
                    'trip_number': {
                        'name': 'trip26',
                        'nullable': True,
                    },
                    'route': {
                        'name': 'route_number',
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
                    'sched_time': {
                        'name': 'departure',
                        'nullable': True,
                    }
                },
                'type': 'SELECT',
                'filters': {
                    'default': [
                        'date'
                    ],
                    'alternate': [
                        'date',
                        'block_number'
                    ]
                },
                'order_by': [
                    'date', 'block_number', 'trip_number', 'sched_time'
                ],
                'table_name': 'v_schedule_stops',
                'database': 'schedule_history',
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
                'type': 'INSERT',
                'table_name': 'service_journal',
                'database': 'segments',
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
                'type': 'SELECT',
                'table_name': 'stops',
                'database': 'Utilities',
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
                        'nullable': False,
                        'do_not_include': True
                    }
                },
                'special_fields': {
                    'seg_path_str': 'CAST(seg_path AS NVARCHAR(4000)) AS seg_path_str'
                },
                'type': 'SELECT',
                'order_by': [
                    'date_created'
                ],
                'table_name': 'segment_dist',
                'database': 'TA_ITHACA_SCHEDULE_HISTORY'

            }
        }
    }
}


def init_config(default_config_, config_name_: str = None):
    global config_name
    if config_name_ is None:
        config_name_ = config_name

    with open(config_name_, 'w') as f:
        json.dump(default_config_, f, indent=4)


def read_config(config_name_: str = None, use_config: bool = True, f_use_config: bool = True):
    """
    Environment variables take priority. (JOURNAL_USE_CONFIG_FILE/JOURNAL_FORCE_USE_CONFIG_FILE/JOURNAL_CONFIG_NAME)
    """
    global config_name
    if config_name_ is None:
        config_name_ = config_name

    use_config = os.environ.get('JOURNAL_USE_CONFIG_FILE', str(use_config)).lower() in ENVIRONMENT_TRUTHY_VALUES
    f_use_config = \
        os.environ.get('JOURNAL_FORCE_USE_CONFIG_FILE', str(f_use_config)).lower() in ENVIRONMENT_TRUTHY_VALUES
    if f_use_config:
        use_config = True
    if use_config:
        try:
            with open(config_name_, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            init_config(DEFAULT_CONFIG)
            print(f'Config initialized as {config_name_}.')
            if f_use_config:
                print(f'Please edit {config_name_} with the appropriate information and restart.')
                sys.exit(1)
            else:
                return DEFAULT_CONFIG
    return DEFAULT_CONFIG


config_name, config, settings, username, password, driver, host, port, attr_sql_map, is_setup = (DEFAULT_CONFIG_NAME,
                                                                                                 None, None, None, None,
                                                                                                 None, None, None, None,
                                                                                                 False)


def setup(force: bool = False):
    """
    If this module is not already setup, sets up global variables using environment variables and the filesystem
    accordingly.

    PARAMETERS
    --------
    force
        Redoes setup even if setup has been called before.
    """
    global config_name, config, settings, username, password, driver, host, port, attr_sql_map, is_setup
    if force or not is_setup:
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
        is_setup = True


if __name__ == '__main__':
    setup()
