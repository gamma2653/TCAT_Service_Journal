# Version 0.1
import pyodbc
import json
import os
import sys
from datetime import date
from service_journal.dbt_classifications import dbt_classes
from service_journal.dbt_classifications.exceptions import BusNotFound, BlockNotFound, TripNotFound
from service_journal.gen_utils.debug import Logger

logger = Logger(__name__)
logger.read_args()

INIT = {
	'settings': {
		'driver': '{ODBC Driver 11 for SQL Server}',
		'host': 'AVAILDEV',
		'username': '',
		'password': '',
		'__comment': 'Warning: Stored passwords are unencrypted.',
		'dbt_sql_map': {
			'actual': {
				'date': {
					'name': 'service_day',
					'view': 'v_vehicle_history',
					'nullable': True
				},
				'blockNumber': {
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
				'time': {
					'name': 'Time',
					'view': 'v_vehicle_history',
					'nullable': True
				},
				# 'dep_time': {
				# 	'name': 'Departure_Time',
				# 	'view': 'v_vehicle_history',
				# 	'nullable': True
				# },
				'route': {
					'name': 'Route',
					'view': 'v_vehicle_history',
					'nullable': True
				},
				'dir': {
					'name': 'dir',
					'view': 'v_vehicle_history',
					'nullable': True
				},
				'stop': {
					'name': 'Stop_Id',
					'view':'v_vehicle_history',
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
				'opStatus': {
					'name': 'OperationalStatus',
					'view': 'v_vehicle_history',
					'nullable': True
				}
			},
			'scheduled': {
				'date': {
					'name': 'calendar',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'blockNumber': {
					'name': 'BlockNumber',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'dir': {
					'name': 'Direction',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'tripNumber': {
					'name': 'Trip26',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'i_stop': {
					'name': 'iStop',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'stop': {
					'name': 'tStop',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'i_stop_name': {
					'name': 'iStopName',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				't_stop_name': {
					'name': 'tStopName',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'time': {
					'name': 'DepartureTime',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'layover': {
					'name': 'layover',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'run': {
					'name': 'RunNumber',
					'view': 'v_sched_trip_stop',
					'nullable': True
				},
				'pieceNumber': {
					'name': 'PieceNumber',
					'view': 'v_sched_trip_stop',
					'nullable': True
				}
			},
			'views': {
				'scheduled': {
					'table': 'v_sched_trip_stop',

					'deflt_query': 'SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM ? WHERE ?=?',

					'opt_query': 'SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM ? WHERE ?=? AND ?=?',

				},
				'actual': {
					'table': 'v_vehicle_history',

					'deflt_query': 'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM ? WHERE ?=?',

					'opt_query': 'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM ? WHERE ?=? AND ?=?',
				}
			},
			'databases': {
				'scheduled': 'TA_ITHACA_SCHEDULE_HISTORY',
				'actual': 'TA_ITHACA_ACTUAL_HISTORY',
				'writeTo' : ''
			}
		},
	}
}


def init_config():
	logger.info('Initializing config...')
	f = open('config.json', 'w')
	f.write(json.dumps(INIT, sort_keys=True, indent=4))
	f.close()
	logger.info('Config has been initalized!')

def read_config():
	logger.fine('Reading config.')
	f = open('config.json', 'r')
	config = json.loads(f.read())
	f.close()
	del f
	logger.fine('Read config.')
	return config

class Connection:
	def close(self):
		try:
			pass
			# self.db.close()
		except NameError:
			pass
	def __del__(self):
		self.close()
		super().__del__()

	def __init__(self, config_path):
		logger.info('Initializing Connection with config path: %s' % (config_path))
		# Open or initialize config into dictionary
		if not os.path.exists(os.path.join(config_path, 'config.json')):
			logger.info('Initializing config for first time!')
			init_config()

		config = read_config()
		self.config = config
		try:
			settings = config['settings']
		except KeyError as e:
			print(('Error: Key (%s) not found in config.json. If this is your first time '
			'running this, please setup your config and re-run it.') % (e.args[0]))
			raise e

		dbt_sql_map = settings['dbt_sql_map'] #load in json
		sql_dbt_map={}
		for t in dbt_sql_map:
			if not t in sql_dbt_map:
				sql_dbt_map[t] = {}
			if t=='actual' or t=='scheduled':
				for field in dbt_sql_map[t]:
					sql_dbt_map[t][dbt_sql_map[t][field]['name']] = {'name':field, 'nullable':dbt_sql_map[t][field]['nullable'], 'view':dbt_sql_map[t][field]['view']}
			else:
				sql_dbt_map[t]=dbt_sql_map[t]
		self.dbt_sql_map = dbt_sql_map
		self.sql_dbt_map = sql_dbt_map
		logger.info('dbt and sql mappings made!')
		self.resetConnection(config)

	def resetConnection(self, config):
		logger.info('Resetting connection to %s.'% (str(config)))
		self.config = config
		settings = config['settings']
		try:
			self.driver = settings['driver']
			self.user = settings['username']
			self.password = settings['password']
			self.databases = self.dbt_sql_map['databases']
			self.host = settings['host']
		except KeyError as e:
			print(('Error: Key (%s) not found in config.json. If this is your first time '
				   'running this, please setup your config and re-run it.') % (e.args[0]))
			raise e
		self.actual_read_conn = pyodbc.connect(r'DRIVER=%s;'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			r'SERVER=%s;'
			r'DATABASE=%s;'
			r'UID=%s;'
			r'PWD=%s'% (self.driver, self.host, self.databases['actual'], self.user, self.password))
		self.scheduled_read_conn = pyodbc.connect(r'DRIVER=%s;'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			r'SERVER=%s;'
			r'DATABASE=%s;'
			r'UID=%s;'
			r'PWD=%s'% (self.driver, self.host, self.databases['scheduled'], self.user, self.password))
		# self.write_conn = pyodbc.connect(r'DRIVER=%s;'
		# 	#The host driver, as list of these can be found on the pyodbc
		# 	#library readme on github
		# 	r'SERVER=%s;'
		# 	r'DATABASE=%s;'
		# 	r'UID=%s;'
		# 	r'PWD=%s'% (self.driver, self.host, self.write_database, self.user, self.password))
		logger.info('Connection successfully set!')

	# Only should be called on
	def selectDate(self, date, block=-1):
		logger.info('Selecting block=%s on day=%s' % (str(date), 'all' if block==-1 else str(block)))
		settings = self.config['settings']
		a_dbt_sql_map = self.dbt_sql_map['actual']
		s_dbt_sql_map = self.dbt_sql_map['scheduled']
		if block==-1:
			tQuery = 'deflt_query'
		else:
			tQuery = 'opt_query'
		aQuery = settings['dbt_sql_map']['views']['actual'][tQuery]
		sQuery = settings['dbt_sql_map']['views']['scheduled'][tQuery]
		aTable = settings['dbt_sql_map']['views']['actual']['table']
		sTable = settings['dbt_sql_map']['views']['scheduled']['table']
		# else:
		# 	query = settings['optSelectScheduledQuery']
		aCursor = self.actual_read_conn.cursor()
		sCursor = self.scheduled_read_conn.cursor()
		if block==-1:
			print(aQuery)
			print(type(aQuery))
			print(a_dbt_sql_map['date'])
			print(type(a_dbt_sql_map['date']))
			aCursor.execute(aQuery, a_dbt_sql_map['date']['name'], \
			 a_dbt_sql_map['blockNumber']['name'], a_dbt_sql_map['tripNumber']['name'],\
			 a_dbt_sql_map['bus']['name'], a_dbt_sql_map['time']['name'], a_dbt_sql_map['route']['name'],\
			 a_dbt_sql_map['dir']['name'], a_dbt_sql_map['stop']['name'], a_dbt_sql_map['name']['name'],\
			 a_dbt_sql_map['boards']['name'], a_dbt_sql_map['alights']['name'],\
			 a_dbt_sql_map['onboard']['name'],a_dbt_sql_map['opStatus']['name'],\
			 aTable, a_dbt_sql_map['date']['name'], date)

			sCursor.execute(sQuery, s_dbt_sql_map['date']['name'], s_dbt_sql_map['blockNumber']['name'],\
			 s_dbt_sql_map['dir']['name'], s_dbt_sql_map['tripNumber']['name'],\
			 s_dbt_sql_map['i_stop']['name'], s_dbt_sql_map['t_stop']['name'],\
			 s_dbt_sql_map['i_stop_name']['name'], s_dbt_sql_map['t_stop_name']['name'],\
			 s_dbt_sql_map['layover']['name'], s_dbt_sql_map['run']['name'],\
			 s_dbt_sql_map['pieceNumber']['name'], sTable, s_dbt_sql_map['date']['name'], date)

		else:
			aCursor.execute(aQuery, query, a_dbt_sql_map['date']['name'], \
			 a_dbt_sql_map['blockNumber']['name'], a_dbt_sql_map['tripNumber']['name'],\
			 a_dbt_sql_map['bus']['name'], a_dbt_sql_map['time']['name'],\
			 a_dbt_sql_map['route']['name'],a_dbt_sql_map['dir']['name'],\
			 a_dbt_sql_map['stop']['name'], a_dbt_sql_map['name']['name'],\
			 a_dbt_sql_map['boards']['name'], a_dbt_sql_map['alights']['name'],\
			 a_dbt_sql_map['onboard']['name'], a_dbt_sql_map['opStatus']['name'],\
			 aTable, a_dbt_sql_map['date']['name'], date, a_dbt_sql_map['blockNumber']['name'], block)

			sCursor.execute(sQuery, s_dbt_sql_map['date']['name'], \
			 s_dbt_sql_map['blockNumber']['name'], s_dbt_sql_map['dir']['name'],\
			 s_dbt_sql_map['tripNumber']['name'], s_dbt_sql_map['i_stop']['name'],\
			 s_dbt_sql_map['t_stop']['name'], s_dbt_sql_map['i_stop_name']['name'],\
			 s_dbt_sql_map['t_stop_name']['name'], s_dbt_sql_map['layover']['name'],\
			 s_dbt_sql_map['run']['name'], s_dbt_sql_map['pieceNumber']['name'],\
			 sTable, s_dbt_sql_map['date']['name'], date, s_dbt_sql_map['blockNumber']['name'], block)
		logger.info('%s successfully selected!' % (str(date)))
		return (aCursor, sCursor)

	def loadData(self, cursors, days=None):
		logger.info('Loading data from cursor.')
		if not days:
			days = dbt_classes.Days()
		aCursor, sCursor = cursors
		# Now for scheduled data
		row = sCursor.fetchone()
		dbt_col_names = [sql_dbt_map[col] for col in sCol_names]
		while row:
			data = dict(zip(dbt_col_names, row))
			days.addDay(data['date'], data['blockNumber'], data['tripNumber'], \
			data['route'], data['direction'], data['stop'], data['name'], \
			data['time'], data['distance'],data['bus'])
			row = cursor.fetchone()
		# Now for ActualData

		row = aCursor.fetchone()
		# cursor_description documentation:
		# 0. column name (or alias, if specified in the SQL)
		# 1. type code
		# 2. display size (pyodbc does not set this value)
		# 3. internal size (in bytes)
		# 4. precision
		# 5. scale
		# 6. nullable (True/False)
		aCol_names = aCursor.description[0]
		aNullable = aCursor.description[6] #Unused at the moment
		sCol_names = sCursor.description[0]
		sNullable = sCursor.description[6] #Unused at the moment
		# get dbt_names for each column for abstraction
		dbt_col_names = [sql_dbt_map[col] for col in aCol_names]
		while row:
			# We zip up our data making a key-value pairing of col_names and rows
			data = dict(zip(dbt_col_names, row))
			days.crossRef(data['date'], data['blockNumber'], data['tripNumber'],\
			 data['stop'],data['bus'],data['boards'],data['alights'],\
			 data['onboard'],data['adjustedOnboard'])
			row = cursor.fetchone()


		logger.info('Date at cursor location loaded!')
		return day
