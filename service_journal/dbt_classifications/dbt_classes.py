#DBT_Classes = Day-Block-Trip classes
import sys
from collections import OrderedDict
from service_journal.dbt_classifications.exceptions import BlockNotFound, TripNotFound, NotActual, NotSchedule
from service_journal.gen_utils.debug import Logger
from datetime import datetime, timedelta
from enum import IntFlag
# Logger initialization
logger = Logger(__name__)
logger.read_args()

class Flag(IntFlag):
	FINE = 0
	BACKWARDS_TIME = 1
	MULTIPLE_BUS_BLOCK = 2

def closestStopID(stop_locations, loc):
	min = sys.maxsize
	closestStop = 0
	for stop_id, stop_loc in stop_locations.items():
		dist = (float(stop_loc[0])-float(loc[0]))**2+(float(stop_loc[1])-float(loc[1]))**2
		if dist<=min:
			closestStop=stop_id
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
		# Make sched_time datetime format
		sched_time = datetime.combine(date, (datetime.min+timedelta(seconds=sched_time)).time())
		stops = self.root[date][blockNumber][tripNumber]['stops']
		# Handle multiple instances of stop
		instance_id = 0
		while (stopID, instance_id) in stops.keys():
			instance_id+=1
		stops[(stopID, instance_id)] = dict({'name':stopName,\
		 'sched_time':sched_time, 'actual_time':None,'distance':distance, 'bus':None,\
		 'avl_boards', 'avl_alights', 'avl_onboard', 'boards':0, 'alights':0, \
		 'onboard':0, 'adjustedOnboard':None, 'seen':0, 'flag':Flag.FINE})


	def crossRef(self, date, blockNumber, tripNumber, stopID, bus, boards, alights, onboard, actual_time, loc, route, stop_locations):
		if date in self.root:
			if blockNumber in self.root[date]:
				if tripNumber in self.root[date][blockNumber]:
					if stopID==0:
						t_stop_locs = {stop_id : stop_locations[stop_id] for \
						(stop_id, instance_id) in self.root[date][blockNumber][tripNumber]['stops'].keys()}
						stopID = closestStopID(stop_locations, loc)
						was197 = True
					else:
						was197 = False
					stops = self.root[date][blockNumber][tripNumber]['stops']
					instance_id = 0
					(l_stopID, _), l_stop_info = stops.items()[0]
					if stopID==l_stopID and l_stop_info['seen']:
						instance_id+=1
					# while stops[(stopID, instance_id)].seen==10 and not was197:
					# 	instance_id+=1
					if stopID in self.root[date][blockNumber][tripNumber]['stops']:
						stop = self.root[date][blockNumber][tripNumber]['stops'][(stopID, instance_id)]
						stop['bus'] = bus
						stop['avl_boards'] = stop['avl_boards']+boards
						stop['avl_alights'] = stop['avl_alights']+alights
						stop['avl_onboard'] = stop['avl_onboard']+onboard
						stop['actual_time'] = actual_time
						stop['seen']+= 1 if was197 else 10
	def postProcess(self):
		buses = {}
		for date in self.root.keys():
			for blockNumber in self.root[date].keys():
				for tripNumber in self.root[date][blockNumber].keys():
					for stopID in self.root[date][blockNunber][tripNumber]['stops'].keys():
						stop = self.root[date][blockNunber][tripNumber]['stops'][stopID]
						if stop['bus']==None:
							pass
							# Check day-bus-block actual times, take start and end time
							# Check for overlap, if overlap, flag overlapping stops
							# Mainly just find a mapping for bus
						else
							if stop['bus'] not in buses:
								buses[stop['bus']] = []
							buses[stop['bus']].append(stop)



	def getStopsByBus(self, bus):
		stops_made = []
		for date in self.root.keys():
			for blockNumber in self.root[date].keys():
				for tripNumber in self.root[date][blockNumber].keys():
					for stopID in self.root[date][blockNunber][tripNumber]['stops'].keys():
						stop = self.root[date][blockNunber][tripNumber]['stops'][stopID]
						if stop['bus'] == bus:
							stops_made.append(stop)
