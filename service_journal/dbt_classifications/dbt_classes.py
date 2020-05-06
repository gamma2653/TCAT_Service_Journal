#DBT_Classes = Day-Block-Trip classes
import sys
from collections import OrderedDict
from service_journal.dbt_classifications.exceptions import BlockNotFound, TripNotFound, NotActual, NotSchedule
from service_journal.gen_utils.debug import Logger
# Logger initialization
logger = Logger(__name__)
logger.read_args()

def closestStopID(stop_locations, loc):
	min = sys.maxsize
	closestStop = 0
	for stop_id, stop_loc in stop_locations:
		dist = (stop_loc[0]-loc[0])**2+(stop_loc[1]-loc[1])**2
		if dist<=min:
			closestStop=stopID
			min = dist
	return closestStop

class Days:
	def __init__(self):
		self.root = dict()
	def addDay(self, date):
		self.root[date] = dict()
	def addBlock(self, date, blockNumber):
		if date not in self.root:
			self.addDay(date)
		self.root[date][blockNumber] = dict()
	def addTrip(self, date, blockNumber, tripNumber, route, direction):
		if date not in self.root:
			self.addDay(date)
		if blockNumber not in self.root[date]:
			self.addBlock(date, blockNumber)
		self.root[date][blockNumber][tripNumber] = dict({'stops':OrderedDict(), 'route':route, 'direction':direction})
	# stopInfo is a tuple containing stopInfo
	def addStop(self, date, blockNumber, tripNumber, route, direction, stopID, stopName, sched_time, distance,):
		if date not in self.root:
			self.addDay(date)
		if blockNumber not in self.root[date]:
			self.addBlock(date, blockNumber)
		if tripNumber not in self.root[date][blockNumber]:
			self.addTrip(date, blockNumber, tripNumber, route, direction)
		self.root[date][blockNumber][tripNumber]['stops'][stopID] = dict({'name':stopName,\
		 'sched_time':sched_time, 'actual_time':None,'distance':distance, 'bus':None, 'boards':0, 'alights':0, \
		 'onboard':0, 'adjustedOnboard':None, 'seen':0})


	def crossRef(self, date, blockNumber, tripNumber, stopID, bus, boards, alights, onboard, actual_time, loc, stop_locations):
		if date in self.root:
			if blockNumber in self.root[date]:
				if tripNumber in self.root[date][blockNumber]:
					if stopID==0:
						t_stop_locs = {stop_id : stop_locations[stop_id] for \
						stop_id in self.root[date][blockNumber][tripNumber]['stops'].keys()}
						stopID = closestStopID(stop_locations, loc)
						was197 = True
					else:
						was197 = False
					if stopID in self.root[date][blockNumber][tripNumber]['stops']:
						stop = self.root[date][blockNumber][tripNumber]['stops'][stopID]
						stop['bus'] = bus
						stop['boards'] = stop['boards']+boards
						stop['alights'] = stop['alights']+alights
						stop['onboard'] = stop['onboard']+onboard
						stop['actual_time'] = actual_time
						stop['seen']+= 1 if was197 else 10
