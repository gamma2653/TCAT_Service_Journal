# Version 0.1
import pyodbc
import json
import os
import sys
from datetime import date, datetime
from service_journal.dbt_classifications import dbt_classes
from service_journal.dbt_classifications.exceptions import BusNotFound, BlockNotFound, TripNotFound
from service_journal.gen_utils.debug import Logger

logger = Logger(__name__)
logger.read_args()

'''
Default SQL
'''
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
				},
				'latitude':{
					'name': 'Latitude',
					'view': 'v_vehicle_history',
					'nullable': True
				},
				'longitude':{
					'name': 'Longitude',
					'view': 'v_vehicle_history',
					'nullable': True
				},
			},
			'scheduled': {
				'date': {
					'name': 'Service_Date',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'direction': {
					'name': 'Direction',
					'view': 'v_vehicle_history',
					'nullable': True
				},
				'blockNumber': {
					'name': 'BlockNumber',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'service_record': {
					'name': 'ServiceRecordId',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'distance': {
					'name': 'dist_ft',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'tripNumber': {
					'name': 'Trip26',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'i_stop': {
					'name': 'iStop',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'stop': {
					'name': 'tStop',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'i_stop_name': {
					'name': 'iStopName',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'stop_name': {
					'name': 'tStopName',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'sched_time': {
					'name': 'DepartureTime',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'layover': {
					'name': 'layover',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'run': {
					'name': 'RunNumber',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'route': {
					'name': 'RouteNumber',
					'view': 'v_date_block_trip_stop',
					'nullable': True
				},
				'pieceNumber': {
					'name': 'PieceNumber',
					'view': 'v_date_block_trip_stop',
					'nullable': True
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
			},
			'views_tables': {
				'actual': {
					'deflt_query': 'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM ? WHERE ?=?',
					'opt_query': 'SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM ? WHERE ?=? AND ?=?',
					'static': 'SELECT [service_day],[block],[trip26],[bus],[Time],[Departure_Time],[Route],[dir],[Stop_Id],[Stop_Name],[Boards],[Alights],[Onboard],[OperationalStatus],[Latitude],[Longitude] FROM [TA_ITHACA_ACTUAL_HISTORY].[dbo].[v_vehicle_history] WHERE [service_day]=? ORDER BY [service_day],[block],[trip26],[Time] asc',
					'table': 'v_vehicle_history',
					'database':  'TA_ITHACA_ACTUAL_HISTORY'
				},
				'scheduled': {
					'deflt_query': 'SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM ? WHERE ?=?',
					'opt_query': 'SELECT ?,?,?,?,?,?,?,?,?,?,?,?,? FROM ? WHERE ?=? AND ?=?',
					'static':'SELECT [Service_Date], [dist_ft], [ServiceRecordId], [BlockNumber], [RouteNumber], [Direction], [Trip26], [iStop], [tStop], [iStopName], [tStopName], [DepartureTime], [layover], [RunNumber], [PieceNumber] FROM [TA_ITHACA_SCHEDULE_HISTORY].[dbo].[v_date_block_trip_stop] WHERE [Service_Date]=? ORDER BY [Service_Date],[BlockNumber],[Trip26],[DepartureTime] asc',
					'table': 'v_date_block_trip_stop',
					'database': 'TA_ITHACA_SCHEDULE_HISTORY'
				},
				'output': {
					'deflt_query': '',
					'opt_query': '',
					'static':'INSERT INTO [dbo].[segments] ([service_date],[bus],[block],[route],[trip],[trip_sequence],[stop_sequence],[direction],[stop_id],[stop_instance],[stop_name],[stop_message_id],[stop_seen],[boards],[alights],[onboard],[adjusted_onboard],[start_time],[end_time],[segment_feet],[segment_seconds],[sched_start_time],[sched_end_time],[feet_times_passengers],[feet_times_adj_passengers], [flag], [run]) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
					'table': 'segments',
					'database': 'segments'
				},
				'stop_locations':{
					'static':'SELECT [stop_num],[latitude],[longitude] FROM [Utilities].[dbo].[stops]',
					'table': 'stops',
					'database': 'Utilities'
				}
			},
		}
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
			self.actual_read_conn.close()
		except NameError:
			pass
		try:
			self.scheduled_read_conn.close()
		except NameError:
			pass
		try:
			self.write_conn.close()
		except NameError:
			pass
# Need to clean this up later
	def __init__(self, config_path):
		logger.info(f'Initializing Connection with config path: {config_path}')
		# Open or initialize config into dictionary
		if not os.path.exists(os.path.join(config_path, 'config.json')):
			logger.info('Initializing config for first time! Please rerun after having set-up your config.')
			init_config()
			sys.exit(0)

		config = read_config()
		self.config = config
		try:
			settings = config['settings']
		except KeyError as e:
			print(f'Error: Key ({e.args[0]}) not found in config.json. If this is your first time '
			'running this, please setup your config and re-run it.')
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
		logger.info(f'Resetting connection to {str(config)}.')
		self.config = config
		settings = config['settings']
		try:
			self.driver = settings['driver']
			self.user = settings['username']
			self.password = settings['password']
			self.host = settings['host']
		except KeyError as e:
			print(f'Error: Key ({e.args[0]}) not found in config.json. If this is your first time '
				   'running this, please setup your config and re-run it.')
			raise e
		self.actual_read_conn = pyodbc.connect(rf'DRIVER={self.driver};'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			rf'SERVER={self.host};'
			rf'DATABASE={self.dbt_sql_map['views_tables']['actual']['database']};'
			rf'UID={self.user};'
			rf'PWD={self.password};')
		self.scheduled_read_conn = pyodbc.connect(rf'DRIVER={self.driver};'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			rf'SERVER={self.host};'
			rf'DATABASE={self.dbt_sql_map['views_tables']['scheduled']['database']};'
			rf'UID={self.user};'
			rf'PWD={self.password};')
		self.write_conn = pyodbc.connect(rf'DRIVER={self.driver};'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			rf'SERVER={self.host};'
			rf'DATABASE={self.dbt_sql_map['views_tables']['output']['database']};'
			rf'UID={self.user};'
			rf'PWD={self.password};')
		self.stop_locations_conn = pyodbc.connect(rf'DRIVER={self.driver};'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			rf'SERVER={self.host};'
			rf'DATABASE={self.dbt_sql_map['views_tables']['stop_locations']['database']};'
			rf'UID={self.user};'
			rf'PWD={self.password};')
		self.load_stop_loc()
		logger.info('Connection successfully set!')

	def load_stop_loc(self):
		'''

		'''
		self.stop_locations = {}
		# grab query string
		query = self.dbt_sql_map['views_tables']['stop_locations']['static']
		# Grab cursor object
		cursor = self.stop_locations_conn.cursor()
		# Execute query
		cursor.execute(query)
		# Grab first row from result
		row = cursor.fetchone()

		# get dbt_names for each column for abstraction
		dbt_col_names = [self.sql_dbt_map['stop_locations'][col[0]]['name'] for col in cursor.description]
		logger.info(f'generated col names: {dbt_col_names}')
		while row:
			logger.finest('Processing an actual row')
			data = dict(zip(dbt_col_names, row))
			self.stop_locations[data['stop_num']] = (data['latitude'], data['longitude'])
			row = cursor.fetchone()


	# Only should be called on
	def selectDate(self, date, block=-1):
		logger.info(f'Selecting block={str(date)} on day={'all' if block==-1 else str(block)}')
		settings = self.config['settings']
		a_dbt_sql_map = self.dbt_sql_map['actual']
		s_dbt_sql_map = self.dbt_sql_map['scheduled']
		if block==-1:
			tQuery = 'deflt_query'
		else:
			tQuery = 'opt_query'
		# aQuery = settings['dbt_sql_map']['views_tables']['actual'][tQuery]
		# sQuery = settings['dbt_sql_map']['views_tables']['scheduled'][tQuery]
		# Temporarily disabled dynamically generated queries.
		aQuery = settings['dbt_sql_map']['views_tables']['actual']['static']
		sQuery = settings['dbt_sql_map']['views_tables']['scheduled']['static']

		aTable = settings['dbt_sql_map']['views_tables']['actual']['table']
		sTable = settings['dbt_sql_map']['views_tables']['scheduled']['table']
		# else:
		# 	query = settings['optSelectScheduledQuery']
		aCursor = self.actual_read_conn.cursor()
		sCursor = self.scheduled_read_conn.cursor()
		if block==-1:
			# Execute primary query
			logger.info(f'Selecting {str(date)} from the schedule.')
			aCursor.execute(aQuery, str(date))
			logger.info(f'Selecting {str(date)} from the history.')
			sCursor.execute(sQuery, str(date))

		else:
			pass
			# Execute optional query
			# aCursor.execute(aQuery, query, a_dbt_sql_map['date']['name'], \
			#  a_dbt_sql_map['blockNumber']['name'], a_dbt_sql_map['tripNumber']['name'],\
			#  a_dbt_sql_map['bus']['name'], a_dbt_sql_map['time']['name'],\
			#  a_dbt_sql_map['route']['name'],a_dbt_sql_map['dir']['name'],\
			#  a_dbt_sql_map['stop']['name'], a_dbt_sql_map['name']['name'],\
			#  a_dbt_sql_map['boards']['name'], a_dbt_sql_map['alights']['name'],\
			#  a_dbt_sql_map['onboard']['name'], a_dbt_sql_map['opStatus']['name'],\
			#  aTable, a_dbt_sql_map['date']['name'], date, a_dbt_sql_map['blockNumber']['name'], block)
			#
			# sCursor.execute(sQuery, s_dbt_sql_map['date']['name'], \
			#  s_dbt_sql_map['blockNumber']['name'], s_dbt_sql_map['dir']['name'],\
			#  s_dbt_sql_map['tripNumber']['name'], s_dbt_sql_map['i_stop']['name'],\
			#  s_dbt_sql_map['t_stop']['name'], s_dbt_sql_map['i_stop_name']['name'],\
			#  s_dbt_sql_map['t_stop_name']['name'], s_dbt_sql_map['layover']['name'],\
			#  s_dbt_sql_map['run']['name'], s_dbt_sql_map['pieceNumber']['name'],\
			#  sTable, s_dbt_sql_map['date']['name'], date, s_dbt_sql_map['blockNumber']['name'], block)
		logger.info(f'{str(date)} successfully selected!')
		return (aCursor, sCursor)

	def loadData(self, cursors, journal=None, process=False):
		logger.info('Loading data from cursor.')
		if not journal:
			journal = dbt_classes.Journal()
		aCursor, sCursor = cursors
		# Now for scheduled data
		row = sCursor.fetchone()

		# cursor.description documentation:
		# 0. column name (or alias, if specified in the SQL)
		# 1. type code
		# 2. display size (pyodbc does not set this value)
		# 3. internal size (in bytes)
		# 4. precision
		# 5. scale
		# 6. nullable (True/False)

		dbt_col_names = [self.sql_dbt_map['scheduled'][col[0]]['name'] for col in sCursor.description]
		logger.info(f'generated col names: {dbt_col_names}')
		# TODO: Checkin with Tom
		prevTrip = None
		while row:
			logger.finest('Processing a scheduled row')
			# We zip up our data making a key-value pairing of col_names and rows
			data = dict(zip(dbt_col_names, row))
			# Capture intial stop on new trips (Schedule is ordered by trip)
			if prevTrip!=data['tripNumber']:
				journal.addStop(data['date'], data['blockNumber'], data['tripNumber'], \
				 data['route'], data['direction'], data['i_stop'], data['i_stop_name'], \
				 data['sched_time'], data['distance'],data['run'])
				prevTrip = data['tripNumber']
			journal.addStop(data['date'], data['blockNumber'], data['tripNumber'], \
			 data['route'], data['direction'], data['stop'], data['stop_name'], \
			 data['sched_time'], data['distance'], data['run'])
			row = sCursor.fetchone()

		# Now for ActualData
		row = aCursor.fetchone()
		# get dbt_names for each column for abstraction
		dbt_col_names = [self.sql_dbt_map['actual'][col[0]]['name'] for col in aCursor.description]
		logger.info(f'generated col names: {dbt_col_names}')
		while row: #Run iff [process] and there is a row
			if process:
				logger.finest('Processing an actual row')
				data = dict(zip(dbt_col_names, row))
				journal.crossRef(data['date'], data['blockNumber'], data['tripNumber'],\
				 data['stop'],data['bus'],data['boards'],data['alights'], data['onboard'],\
				 data['actual_time'], (data['latitude'], data['longitude']), data['route'], self.stop_locations)
			else:
				logger.finest('Filling in an actual row')
				data = dict(zip(dbt_col_names, row))
				journal.fillIn(data['date'], data['blockNumber'], data['tripNumber'],\
				 data['stop'],data['bus'],data['boards'],data['alights'], data['onboard'],\
				 data['actual_time'], (data['latitude'], data['longitude']), data['route'])
			row = aCursor.fetchone()
		logger.info('Date at cursor location loaded!')
		return journal
	def selectAndLoad(self, date, journal=None, process=False):
		'''Convenience method to select and load from the TCAT database [date] and to store in dbt dictionary format.'''
		return self.loadData(self.selectDate(date), journal=journal, process=process)
	def writeDays(self, journal):
		query = self.sql_dbt_map['views_tables']['output']['static']
		cursor = self.write_conn.cursor()
		for date, journal in journal.root.items():
			for blockNumber, block in journal.items():
				for tripNumber, trip in block.items():
					for (stopNumber, stopInstance), stop in trip['stops'].items():
						cursor.execute(query, date, stop['bus'], blockNumber, \
						 trip['route'], tripNumber, None, None, trip['direction'],\
						 stopNumber, stopInstance, stop['name'], None, stop['seen'], \
						 stop['boards'], stop['alights'], stop['onboard'], \
						 stop['adjustedOnboard'], stop['actual_time'], None, stop['distance'], \
						 None, stop['sched_time'], None, \
						 (0 if stop['distance']==None else stop['distance'])*stop['onboard'],\
						  None, stop['flag'] | trip['flag'], stop['run'])
		cursor.commit()
