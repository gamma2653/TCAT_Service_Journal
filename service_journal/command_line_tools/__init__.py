import sys
from service_journal.gen_utils import debug
from service_journal.sql_handler import connection
from service_journal.dbt_classifications.dbt_classes import Days
from datetime import date
logger = debug.Logger(__name__)
logger.read_args()

# def init_terminal():
#
_path = '.'

def daterange(start_date, end_date):
	for n in range(int ((end_date - start_date).days)):
		yield day = start_date + timedelta(n)

def run():
	while True:
		userin = input('Please input the range of service dates as intergers in \
		the format "year month day - year month day"')
		ins = userin.split('-')
		_from = ins[0].strip().split()
		_to = ins[1].strip().split()
		y1 = _from[0]
		m1 = _from[1]
		d1 = _from[2]
		y2 = _to[0]
		m1 = _to[1]
		d1 = _to[2]
		from_date = date(y1, m1, d1)
		to_date = date(y2, m2, d2)
		conn = connection.Connection(_path)
		days = Days()
		for day in daterange(from_date, to_date):
			days = conn.selectAndLoad(day, days=days)
		conn.writeDays(days)
		conn.close()
		userin = input('Would you like to continue? (Y/n)')
		if userin.strip().lower() == 'n':
			break
