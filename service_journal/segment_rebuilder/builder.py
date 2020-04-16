import sys
from service_journal.dbt_classifications import dbt_classes
from service_journal.gen_utils.debug import Logger
# from service_journal.dbt_classifications.exceptions import InformationMismatch
logger = Logger(__name__)
logger.read_args()

def compare(day, opts):
	seenBlocks = day.getBlockNumbers()
	scheduledBlocks = day.getBlockNumbers(actual = False)

	# Scheduled blocks not visited.
	missedBlocks = scheduledBlocks.difference(seenBlocks)
	# Blocks that were not scheduled that were visited.
	deviatedBlocks = seenBlocks.difference(scheduledBlocks)

	seenTrips = day.getTripNumbers()
	scheduledTrips = day.getTripNumbers(actual = False)

	# Scheduled trips not visited.
	missedTrips = scheduledTrips.difference(seenTrips)
	# Trips that were not scheduled that were visited.
	deviatedTrips = seenTrips.difference(scheduledTrips)

	# Cycle through actuals and match to historical.
	for bus in day.buses:
		for seenTrip in bus.trips:
			scheduledTrip = day.resolveScheduledTrip(seenTrip.tripNumber)
			if(len(scheduledTrip)>1):
				logger.warn('Multiple instances of same trip detected.')

			seenStops = seenTrip.get_stopIDs()
			scheduledStops
			for seenStop in seenTrip.stops:
				
