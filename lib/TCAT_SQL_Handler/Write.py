def getWriteConnection(user, password, server = 'AVAILDEV', database = 'Segments'):
	return pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'		#The server driver, as list of these can be found on the pyodbc library readme on github
		r'SERVER=%s;'							   #need server name here
		r'DATABASE=%s;'							#need database name here
		r'UID=%s;'
		r'PWD=%s'% (server, database, user, password))

def writeToSegments(actualDay, connection):
	Debug.debug('Writing to segments.')
	wCursor = connection.cursor()
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
					wCursor.execute(' INSERT INTO dbo.Segments (ServiceDate, Bus, Block, Route, Trip, Pattern, Direction, iStopID, iStopName, iStopMessageID, iStopSeen, tStopID, tStopName, tStopMessageID, tStopSeen, Boards, Alights, Onboard, AdjustedOnboard, SegmentSequence, StartTime, EndTime, SegmentFeet, trip_sequence, Feet_times_passengers)'
						' VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', day, bus, blockNumber,route, tripNumber,pattern,direction, iStopNumber,iStopName,iStopType, iStopSeen, tStopNumber,tStopName,tStopType, tStopSeen, boards, alights, onboard, adjOnboard, segseq, iStopTime, tStopTime, segDistance, tripseq, passenger_per_feet)
				except Exception as e:
					Debug.debug('Was unable to write the following record to dbo.Segments', record=
						'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'%(day, bus, blockNumber,route, tripNumber,pattern,direction, iStopNumber,iStopName,iStopType, iStopSeen, tStopNumber,tStopName,tStopType, tStopSeen, boards, alights, -1, -1, segseq, iStopTime, tStopTime, segDistance), exception=e)
	connection.commit()
	Debug.debug('Done writing.')
def writeToDeviations(actualDay, connection):
	Debug.debug('Writing to deviations.')
	wCursor = connection.cursor()
	for d in actualDay.deviations:
		# Debug.dev_debug('Here are the deviations...', deviation=d)
		deviant = sanitize(d.deviant.value)
		blockNumber = sanitize(d.block)
		tripNumber = sanitize(d.trip)
		iStop = sanitize(d.iStop)
		tStop = sanitize(d.tStop)
		segment = sanitize(d.segment)
		bus = sanitize(d.bus)
		description = sanitize(d.description)
		service_date = sanitize(d.service_date)
		try:
			wCursor.execute(' INSERT INTO dbo.deviation (deviation_number, block, trip, iStop, tStop, segment, bus, description, service_date)'
				' VALUES(?,?,?,?,?,?,?,?,?)', deviant, blockNumber, tripNumber, iStop, tStop, segment, bus, description, service_date)
		except Exception as e:
			Debug.debug('Was unable to write the following record to dbo.deviation', record=
				'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)'%(deviant, blockNumber, tripNumber, iStop, tStop, segment, bus, description, service_date), exception=e)
			return
	connection.commit()
	Debug.debug('Done writing.')
