# import sys
from service_journal.gen_utils import debug
from service_journal.sql_handler import connection
# from service_journal.dbt_classifications.dbt_classes import Journal
from datetime import date, timedelta

# TODO: Will just be __init__, change to name
logger = debug.Logger(__name__)
logger.read_args()

# def init_terminal():
#
_path = '.'


def date_range(start_date, end_date):
	for n in range(int((end_date - start_date).days)):
		yield start_date + timedelta(n)


def run_days(date_):
	ins = date_.split('-')
	_from = ins[0].strip().split()
	_to = ins[1].strip().split()
	y1 = int(_from[0].strip())
	m1 = int(_from[1].strip())
	d1 = int(_from[2].strip())
	y2 = int(_to[0].strip())
	m2 = int(_to[1].strip())
	d2 = int(_to[2].strip())
	from_date = date(y1, m1, d1)
	to_date = date(y2, m2, d2)
	with connection.Connection(_path) as conn:
		for day in date_range(from_date, to_date):
			days = conn.selectAndLoad(day)
			days.infer_stops()
			days.flag_deviations()
			conn.writeDays(days)
			del days


def run():
	while True:
		user_in = input('Please input the range of service dates as integers in the format.\
		\n"YEAR MONTH DAY - YEAR MONTH DAY":\n')
		ins = user_in.split('-')
		_from = ins[0].strip().split()
		_to = ins[1].strip().split()
		y1 = int(_from[0].strip())
		m1 = int(_from[1].strip())
		d1 = int(_from[2].strip())
		y2 = int(_to[0].strip())
		m2 = int(_to[1].strip())
		d2 = int(_to[2].strip())
		from_date = date(y1, m1, d1)
		to_date = date(y2, m2, d2)
		conn = connection.Connection(_path)
		# TODO: Have days work as day(s)
		for day in date_range(from_date, to_date):
			days = conn.selectAndLoad(day)
			days.infer_stops()
			days.flag_deviations()
			conn.writeDays(days)
			del days
		conn.close()
		user_in = input('Would you like to continue? (Y/n)')
		if user_in.strip().lower() == 'n':
			break
