import sys
from service_journal.dbt_classifications import dbt_classes
from service_journal.gen_utils.debug import Logger
from service_journal.dbt_classifications.exceptions import InformationMismatch
logger = Logger(__name__)
logger.read_args()

def compare(days, opts):
	aDay, sDay = days
	if aDay.date != sDay.date:
		raise InformationMismatch('Actual date is %s while schedueld date is %s'\
		 % (aDay.date, sDay.date))
	
