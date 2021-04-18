# import sys
from service_journal.gen_utils.debug import get_default_logger
from service_journal.sql_handler import connection
from service_journal.classifications.dbt_classes import Journal
from datetime import date, timedelta

logger = get_default_logger(__name__)


def date_range(start_date, end_date):
	for n in range(int((end_date - start_date).days)):
		yield start_date + timedelta(n)


def run_days(dates_, config=None, hold_data=False):
	ins = dates_.split('-')
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
	with Journal(config) as journal:
		if hold_data:
			journal.read_days(date_range=date_range(from_date, to_date))
			journal.process()
			journal.write()
		else:
			for day in date_range(from_date, to_date):
				journal.clear()
				journal.read_day(day)
				journal.process()
				journal.write()


def run():
	while True:
		user_in = input('Please input the range of service dates as integers in the format.\
		\n"YEAR MONTH DAY - YEAR MONTH DAY":\n')
		run_days(user_in)
		user_in = input('Would you like to continue? (Y/n)')
		if user_in.strip().lower() == 'n':
			break
