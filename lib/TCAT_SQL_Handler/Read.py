import pyodbc

def getReadConnection(user, password, server = 'AVAILDEV',
 	database = 'Utilities'):
	return pyodbc.connect(r'DRIVER={ODBC Driver 11 for SQL Server};'
		#The server driver, as list of these can be found on the pyodbc
		#library readme on github
		r'SERVER=%s;'
		r'DATABASE=%s;'
		r'UID=%s;'
		r'PWD=%s'% (server, database, user, password))

selectActualInformation = ( ' SELECT  Message_Type_Id, service_date, block,'
							' route, trip, dir, vmh_time, bus, Onboard, boards,'
							' alights, Stop_Id,Internet_Name '
							' FROM dbo.vActual_History'
							' WHERE service_date =? '
							' AND Message_Type_Id!=152'
							'ORDER BY vmh_time asc')

selectScheduledInformation = ( ' SELECT service_day,trip_seq, stop_seq, route,'
							' trip, direction, block,Pattern_Record_Id,'
							' trip_start, trip_end, iStop_Id, tStop_Id,'
							' iStop_descr,tStop_descr, segment_feet'
							' FROM dbo.vHistorical_Stop_Schedule '
							' WHERE service_day = ?'
							' ORDER BY trip_seq, stop_seq asc')
