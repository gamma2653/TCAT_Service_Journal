# Version 0.1
import pyodbc
import json
import os

def init_config():
	init = {
	    "Settings": {
	        "ActualView": "",
	        "Database": "",
	        "Driver": "{ODBC Driver 11 for SQL Server}",
	        "Host": "AVAILDEV",
	        "Password": "",
	        "ScheduledView": "",
	        "Username": "",
	        "__comment": "Warning: Stored passwords are unencrypted.",
	        "dbt_sql_map": {
	            "blockNumber": {
					"name" : "BlockNumber",
					"nullable" : True
					},
	            "date": {
					"name": "Service_Date",
					"nullable" : True
				},
	            "distance": {
					"name": "dist_ft",
					"nullable" : True
				},
	            "end": {
					"name": "seg_end",
					"nullable" : True
				},
	            "iStopID": {
					"name": "iStop",
					"nullable" : True
				},
	            "iStopName": {
					"name": "iName",
					"nullable" : True
				},
	            "mileage": {
					"name": "trip_mileage",
					"nullable" : True
				},
	            "piece_number": {
					"name": "PieceNumber",
					"nullable" : True
				},
	            "run_number": {
					"name": "RunNumber",
					"nullable" : True
				},
	            "segSeq": {
					"name": "seg_seq",
					"nullable" : True
				},
	            "start": {
					"name": "seg_start",
					"nullable" : True
				},
	            "tStopID": {
					"name": "tStop",
					"nullable" : True
				},
	            "tStopName": {
					"name": "tName",
					"nullable" : True
				},
	            "tripNumber": {
					"name": "trip",
					"nullable" : True
				},
	            "tripNumber26": {
					"name": "trip26",
					"nullable" : True
				},
	            "trip_minutes": {
					"name": "trip_minutes",
					"nullable" : True
				},
	        },
	        "optSelectScheduledQuery": [
	            "SELECT Message_Type_Id, service_date, block, route, dir, trip, vmh_time, bus, Deviation, Onboard, Boards, Alights, Stop_Id, Stop_Name, Departure_Time, Latitude, Longitude FROM dbo.v_actual_block_trip_stop WHERE service_date = ? AND block = ? ORDER BY vmh_time asc",
	            "Service_Date",
	            "BlockNumber"
	        ],
	        "selectScheduledQuery": [
	            "SELECT Message_Type_Id, service_date, block, route, dir, trip, vmh_time, bus, Deviation, Onboard, Boards, Alights, Stop_Id, Stop_Name, Departure_Time, Latitude, Longitude FROM dbo.v_actual_block_trip_stop WHERE service_date = ? ORDER BY vmh_time asc",
	            "Service_Date"
	        ]
	    }
	}

	f = open('config.json', 'w')
	f.write(json.dumps(init, sort_keys=True, indent=4))

def read_config():
	global config, settings
	f = open('config.json', 'r')
	config = json.loads(f.read())
	f.close()
	del f
	return config


# Open or initialize config into dictionary
if not os.path.exists('config.json'):
	init_config()

config = read_config()
settings = config['Settings']
writeDatabase = settings['ActualView']
readDatabase = settings['ScheduledView']

dbt_sql_map = settings['dbt_sql_map'] #load in json
sql_dbt_map = {value['name'] : {'name' : key, 'nullable' : value['nullable']} for (key, value) in dbt_sql_map.items()}
# test = {value["name"]:{"name" : key, "nullable": value["nullable"]} for key:value in "dbt_sql_map"}
class TCATConnection:

	def close(self):
		try:
			self.db.close()
		except NameError:
			pass
	def __del__(self):
		self.close()

	def resetConnection(self, driver=settings['Driver'], user=settings['Username'],
		  password=settings['Password'], database=settings['Database'],
		  host=settings['Host']):
		self.user = user
		self.password = password
		self.database = database
		self.host = host
		self.db = pyodbc.connect(r'DRIVER=%s;'
			#The host driver, as list of these can be found on the pyodbc
			#library readme on github
			r'SERVER=%s;'
			r'DATABASE=%s;'
			r'UID=%s;'
			r'PWD=%s'% (host, database, user, password))
	def __init__(self, driver=settings['Driver'], user=settings['Username'],
		  password=settings['Password'], database=settings['Database'],
		  host=settings['Host']):
		self.resetConnection(driver,user,password,database,host)

	# Only should be called on
	def selectActualDate(self, date, block=-1):
		if block==-1:
			query = settings['selectScheduledQuery']
		else:
			query = settings['optSelectScheduledQuery']
		cursor = self.db.cursor()
		cursor.execute(query, date, block)
		return cursor
	def loadData(self, cursor, day=None):
		cursor = self.cursor
		retday = day if day else day
		if day:
			pass
		else:
			row = cursor.fetchone()
			# cursor_description documentation:
			# 0. column name (or alias, if specified in the SQL)
			# 1. type code
			# 2. display size (pyodbc does not set this value)
			# 3. internal size (in bytes)
			# 4. precision
			# 5. scale
			# 6. nullable (True/False)
			col_names = row.cursor_description[0]
			nullable = row.cursor_description[6]
			print(col_names)
			print(nullable)
			while row:

				# Have to login to avl to do some testing to see if col_names is
				# a tuple of column names
				row = cursor.fetchone()
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
						# 	'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'%(day, bus, blockNumber,route, tripNumber,pattern,direction, iStopNumber,iStopName,iStopType, iStopSeen, tStopNumber,tStopName,tStopType, tStopSeen, boards, alights, -1, -1, segseq, iStopTime, tStopTime, segDistance), exception=e)
		self.db.commit()
