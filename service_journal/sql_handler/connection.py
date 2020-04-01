# Version 0.1
import pyodbc
import json
import os
import sys
from service_journal.dbt_classifications import dbt_classes
from service_journal.dbt_classifications.exceptions import BusNotFound, BlockNotFound, TripNotFound
from service_journal.gen_utils.debug import Logger

if '-ll' in sys.argv:
	try:
		level = sys.argv[sys.argv.index('-ll')+1]
		logger = Logger(__name__)
		logger.set_listen_level(level)
	except IndexError:
		logger = Logger(__name__)
		logger.error('Logger was not able to be initialized to specified level. -ll Must be followed by Level Code.')
elif '--log-level' in sys.argv:
	try:
		level = sys.argv[sys.argv.index('--log-level')+1]
		logger = Logger(__name__)
		logger.set_listen_level(level)
	except IndexError:
		logger = Logger(__name__)
		logger.error('Logger was not able to be initialized to specified level. --log-level Must be followed by Level Code.')
else:
	logger = Logger(__name__)


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
			}
		},
	}
}


def init_config():
	f = open('config.json', 'w')
	f.write(json.dumps(INIT, sort_keys=True, indent=4))
	f.close()

def read_config():
	f = open('config.json', 'r')
	config = json.loads(f.read())
	f.close()
	del f
	return config

class Connection:
	def close(self):
		try:
			self.db.close()
		except NameError:
			pass
	def __del__(self):
		self.close()
		self.super().__del__()

	def __init__(self, config_path):
		debug.print_d('', module=__name__, level=debug.INFO)
		# Open or initialize config into dictionary
		if not os.path.exists(config_path, 'config.json'):
			init_config()

		config = read_config()
		try:
			settings = config['Settings']
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
		self.resetConnection(config)

	def resetConnection(self, config):
		try:
			self.driver = config['driver']
			self.user = config['username']
			self.password = config['password']
			self.databases = self.dbt_sql_map['databases']
			self.host = config['host']
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
		self.write_conn = pyodbc.connect(r'DRIVER=%s;'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			r'SERVER=%s;'
			r'DATABASE=%s;'
			r'UID=%s;'
			r'PWD=%s'% (self.driver, self.host, self.write_database, self.user, self.password))

	# Only should be called on
	def selectDate(self, date, block=-1):
		if block==-1:
			aQuery = settings['dbt_sql_map']['views']['v_vehicle_history']['deflt_query']
			sQuery = settings['dbt_sql_map']['views']['v_sched_trip_stop']['deflt_query']
		else:
			aQuery = settings['dbt_sql_map']['views']['v_vehicle_history']['opt_query']
			sQuery = settings['dbt_sql_map']['views']['v_sched_trip_stop']['opt_query']
		# else:
		# 	query = settings['optSelectScheduledQuery']
		aCursor = self.actual_read_conn.cursor()
		sCursor = self.scheduled_read_conn.cursor()
		if block==-1:
			aCursor.execute(aQuery, query, dbt_sql_map['date'], \
			dbt_sql_map['blockNumber'], dbt_sql_map['tripNumber'],\
			dbt_sql_map['bus'], dbt_sql_map['time'], dbt_sql_map['route'],\
			dbt_sql_map['dir'], dbt_sql_map['stop'], dbt_sql_map['name'],\
			dbt_sql_map['boards'], dbt_sql_map['alights'], dbt_sql_map['onboards'],\
			dbt_sql_map['opStatus'], table, dbt_sql_map['date'], date)

			sCursor.execute(sQuery, dbt_sql_map['date'], dbt_sql_map['blockNumber'],\
			dbt_sql_map['dir'], dbt_sql_map['tripNumber'], dbt_sql_map['i_stop'],\
			dbt_sql_map['t_stop'], dbt_sql_map['i_stop_name'],\
			dbt_sql_map['t_stop_name'], dbt_sql_map['layover'], dbt_sql_map['run'],\
			dbt_sql_map['pieceNumber'], table, dbt_sql_map['date'], date)

		else:
			aCursor.execute(aQuery, query, dbt_sql_map['date'], \
			dbt_sql_map['blockNumber'], dbt_sql_map['tripNumber'],\
			dbt_sql_map['bus'], dbt_sql_map['time'], dbt_sql_map['route'],\
			dbt_sql_map['dir'], dbt_sql_map['stop'], dbt_sql_map['name'],\
			dbt_sql_map['boards'], dbt_sql_map['alights'], dbt_sql_map['onboards'],\
			dbt_sql_map['opStatus'], table, dbt_sql_map['date'], date,\
			dbt_sql_map['blockNumber'], block)

			sCursor.execute(sQuery, dbt_sql_map['date'], dbt_sql_map['blockNumber'],\
			dbt_sql_map['dir'], dbt_sql_map['tripNumber'], dbt_sql_map['i_stop'],\
			dbt_sql_map['t_stop'], dbt_sql_map['i_stop_name'],\
			dbt_sql_map['t_stop_name'], dbt_sql_map['layover'], dbt_sql_map['run'],\
			dbt_sql_map['pieceNumber'], table, dbt_sql_map['date'], date,\
			dbt_sql_map['blockNumber'], block)
		return (aCursor, sCursor)

	def loadData(self, cursors, day=None):


		aCursor, sCursor = cursors

		# Start with ActualData

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
		aNullable = aCursor.description[6]
		sCol_names = sCursor.description[0]
		sNullable = sCursor.description[6]
		# get dbt_names for each column for abstraction
		dbt_col_names = [sql_dbt_map[col] for col in aCol_names]
		while row:
			# We zip up our data making a key-value pairing of col_names and rows
			data = dict(zip(dbt_col_names, row))
			if not day:
				day = dbt_classes.Day(data['date'])
			elif day.date != data['date']:
				logger.warn('Date mismatch, reading %s into a day with %s.' % (data['date'], day.date))	
			# This would be for scheduled
			# try:
			# 	block = day.getBlock(data['blockNumber'])
			# except BlockNotFound:
			# 	block = dbt_classes.Block(data['blockNumber'])
			# 	day.addBlock(block)
			try:
				bus = day.getBus(data['bus'])
			except BusNotFound:
				bus = dbt_classes.Bus(data('bus'))
				day.addBus(bus)
			try:
				trip = bus.getTrip(data['tripNumber'])
			except TripNotFound:
				trip = dbt_classes.Trip(data['tripNumber'], seq, data['route'], data['dir'])
			trip.addStop(Stop(-1, data['stop'], data['name'], data['time'], -1))
			# Will replace -1's when I hear back.
			# -1 Values have to inquire about.
			# Have to login to avl to do some testing to see if col_names is
			# a tuple of column names
			row = cursor.fetchone()

		# Now for scheduled data

		row = sCursor.fetchone()
		dbt_col_names = [sql_dbt_map[col] for col in sCol_names]
		while row:
			data = dict(zip(dbt_col_names, row))
			if not day:
				day = dbt_classes.Day()
			day.date = data['date']
			try:
				block = day.getBlock(data['blockNumber'])
			except BlockNotFound:
				block = dbt_classes.Block(data('blockNumber'))
				day.addBlock(bus)
			try:
				trip = block.getTrip(data['tripNumber'])
			except TripNotFound:
				trip = dbt_classes.Trip(data['tripNumber'], seq, data['route'], data['dir'])
			trip.addStop(Stop(-1, data['stop'], data['name'], data['time'], -1))
			row = cursor.fetchone()

		return day
	# [actualDay] is an ActualDay being written to the database
	def writeToRebuiltSegments(actualDay, connection):
		cursor = self.db.cursor()
		for b in actualDay.blocks:
			for t in b.trips:
				for s in t.segments:
					day=actualDay.date
					bus=s.bus
					blockNumber=b.blockNumber
					route=t.route
					tripNumber=t.tripNumber
					direction=t.direction
					iStopNumber=s.segmentID[0].stopID
					iStopName=s.segmentID[0].stopName
					iStopType=s.segmentID[0].messageTypeID
					iStopSeen=s.segmentID[0].seen
					tStopNumber=s.segmentID[1].stopID
					tStopName=s.segmentID[1].stopName
					tStopType=s.segmentID[1].messageTypeID
					tStopSeen=s.segmentID[1].seen
					boards=s.segmentID[0].boards
					alights=s.segmentID[1].alights
					onboard=s.onboard
					adjOnboard=s.adjustedOnboard
					iStopTime=s.segmentID[0].stopTime
					tStopTime=s.segmentID[1].stopTime
					segDistance=s.distance
					pattern=t.pattern
					segseq=s.segmentSeq
					tripseq=t.tripSeq
					try:
						passenger_per_feet=s.feet_per_passenger
					except Exception:
						passenger_per_feet=None
					try:
						cursor.execute((' INSERT INTO dbo.Segments (ServiceDat'
						'e, Bus, Block, Route, Trip, Pattern, Direction, iStop'
						'ID, iStopName, iStopMessageID, iStopSeen, tStopID, tS'
						'topName, tStopMessageID, tStopSeen, Boards, Alights, '
						'Onboard, AdjustedOnboard, SegmentSequence, StartTime,'
						' EndTime, SegmentFeet, trip_sequence, Feet_times_pass'
						'engers) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'
						'?,?,?,?,?,?)'), day, bus, blockNumber, route, tripNumber,
						pattern,direction,iStopNumber,iStopName,iStopType,
						iStopSeen, tStopNumber,tStopName,tStopType, tStopSeen,
						boards, alights, onboard, adjOnboard, segseq, iStopTime,
						tStopTime, segDistance, tripseq, passenger_per_feet)
					except Exception as e:
						print('Exception has occurred when trying to write to self.\n%s' % (e))
						# Debug.debug('Was unable to write the following record to dbo.Segments', record=
						#     'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'%(day, bus, blockNumber,route, tripNumber,pattern,direction, iStopNumber,iStopName,iStopType, iStopSeen, tStopNumber,tStopName,tStopType, tStopSeen, boards, alights, -1, -1, segseq, iStopTime, tStopTime, segDistance), exception=e)
		self.db.commit()
