import os
import json

default_config = {
    'settings': {
        # The host driver, as list of these can be found on the pyodbc library
        # readme on github
        'driver': '{ODBC Driver 11 for SQL Server}',
        'host': 'AVAILDEV',
        'username': '',
        'password': '',
        '__comment': 'Warning: Stored passwords are unencrypted. For testing purposes only',
        'dbt_sql_map': {
            'actual': {
                'date': {
                    'name': 'service_day',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'block_number': {
                    'name': 'block',
                    'view': 'v_vehicle_history',
                    'nullable': True,
                },
                'tripNumber': {
                    'name': 'trip26',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'bus': {
                    'name': 'bus',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'trigger_time': {
                    'name': 'Time',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'actual_time': {
                    'name': 'Departure_Time',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'route': {
                    'name': 'Route',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'direction': {
                    'name': 'dir',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'stop': {
                    'name': 'Stop_Id',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'name': {
                    'name': 'Stop_Name',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'boards': {
                    'name': 'Boards',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'alights': {
                    'name': 'Alights',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'onboard': {
                    'name': 'Onboard',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'op_status': {
                    'name': 'OperationalStatus',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'latitude': {
                    'name': 'Latitude',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'longitude': {
                    'name': 'Longitude',
                    'view': 'v_vehicle_history',
                    'nullable': True
                },
                'operator': {
                    'name': 'Operator_Record_Id',
                    'view': 'v_vehicle_history',
                    'nullable': True
                }
            },
            'scheduled': {
                'date': {
                    'name': 'Service_Date',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                'direction': {
                    'name': 'Direction',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                'block_number': {
                    'name': 'block',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                # 'service_record': {
                # 	'name': 'ServiceRecordId',
                # 	'view': 'v_date_block_trip_stop',
                # 	'nullable': True
                # },
                # 'distance': {
                # 	'name': 'dist_ft',
                # 	'view': 'v_date_block_trip_stop',
                # 	'nullable': True
                # },
                'trip_number': {
                    'name': 'trip26',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                'stop': {
                    'name': 'stop_num',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                'next_stop': {
                    'name': 'next_stop',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                # 'i_stop_name': {
                # 	'name': 'iStopName',
                # 	'view': 'v_date_block_trip_stop',
                # 	'nullable': True
                # },
                # 'stop_name': {
                # 	'name': 'tStopName',
                # 	'view': 'v_date_block_trip_stop',
                # 	'nullable': True
                # },
                'sched_time': {
                    'name': 'departure_time',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                # 'layover': {
                # 	'name': 'layover',
                # 	'view': 'v_date_block_trip_stop',
                # 	'nullable': True
                # },
                # 'run': {
                # 	'name': 'RunNumber',
                # 	'view': 'v_date_block_trip_stop',
                # 	'nullable': True
                # },
                'route': {
                    'name': 'RouteNumber',
                    'view': 'v_scheduled_stops',
                    'nullable': True
                },
                # 'pieceNumber': {
                # 	'name': 'PieceNumber',
                # 	'view': 'v_date_block_trip_stop',
                # 	'nullable': True
                # }
            },
            'views_tables': {
                'actual': {
                    'deflt_query': 'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM ? WHERE ?=?',
                    'opt_query': 'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM ? WHERE ?=? AND ?=?',
                    'static': 'SELECT [service_day],[block],[trip26],[bus],[Time],[Departure_Time],[Route],[dir],'
                              '[Stop_Id],[Stop_Name],[Boards],[Alights],[Onboard],[OperationalStatus],[Latitude],'
                              '[Longitude],[Operator_Record_Id] FROM '
                              '[TA_ITHACA_ACTUAL_HISTORY].[dbo].[v_vehicle_history] WHERE [service_day]=? ORDER BY '
                              '[service_day],[block],[trip26],[Time] asc',
                    'table': 'v_vehicle_history',
                    'database': 'TA_ITHACA_ACTUAL_HISTORY'
                },
                'scheduled': {
                    'deflt_query': 'SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM ? WHERE ?=?',
                    'opt_query': 'SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM ? WHERE ?=? AND ?=?',
                    'static': 'SELECT [Service_Date], [Direction], [block], [trip26], [stop_num], [next_stop], '
                              '[departure_time], [route] FROM [TA_ITHACA_SCHEDULE_HISTORY].[dbo].[v_scheduled_stops] '
                              'WHERE [Service_Date]=? ORDER BY [Service_Date],[BlockNumber],[Trip26],[DepartureTime] '
                              'asc',
                    'table': 'v_scheduled_stops',
                    'database': 'TA_ITHACA_SCHEDULE_HISTORY'
                },
                'output': {
                    'deflt_query': '',
                    'opt_query': '',
                    'static': 'INSERT INTO [dbo].[segments] ([service_date],[bus],[block],[route],[trip],'
                              '[trip_sequence],[stop_sequence],[direction],[stop_id],[stop_instance],[stop_name],'
                              '[stop_message_id],[stop_seen],[boards],[alights],[onboard],[adjusted_onboard],'
                              '[start_time],[end_time],[segment_feet],[segment_seconds],[sched_start_time],'
                              '[sched_end_time],[feet_times_passengers],[feet_times_adj_passengers], [flag], [run])'
                              ' VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    'table': 'segments',
                    'database': 'segments'
                },
                'stop_locations': {
                    'static': 'SELECT [stop_num],[latitude],[longitude] FROM [Utilities].[dbo].[stops]',
                    'table': 'stops',
                    'database': 'Utilities'
                }
            }
        },
        'stop_locations': {
            'stop_num': {
                'name': 'stop_num',
                'view': 'stop_locations',
                'nullable': False
            },
            'latitude': {
                'name': 'latitude',
                'view': 'stop_locations',
                'nullable': False
            },
            'longitude': {
                'name': 'longitude',
                'view': 'stop_locations',
                'nullable': False
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
# I should simplify the format of dbt_sql_map to make inverting easier.
dbt_sql_map = settings['dbt_sql_map']
