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
		self.root[date][blockNumber][tripNumber] = OrderedDict({'stops':OrderedDict(), 'route':route, 'direction':direction, 'actual_start':None, 'actual_end':None, 'flag':Flag.FINE})
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
		 'avl_boards':0, 'avl_alights':0, 'avl_onboard':0, 'boards':0, 'alights':0, \
		 'onboard':0, 'adjustedOnboard':None, 'seen':0, 'flag':Flag.FINE})


	def crossRef(self, date, blockNumber, tripNumber, stopID, bus, boards, alights, onboard, actual_time, loc, route, stop_locations):
		if date in self.root:
			day = self.root[date]
			if blockNumber in day:
				block = day[blockNumber]
				if tripNumber in block:
					trip = block[tripNumber]
					stops = trip['stops']
					if stopID==0:
						t_stop_locs = {stop_id : stop_locations[stop_id] for \
						(stop_id, instance_id) in stops.keys()}
						stopID = closestStopID(stop_locations, loc)
						was197 = True
					else:
						was197 = False
					instance_id = 0
					(l_stopID, _), l_stop_info = list(stops.items())[0]
					if stopID==l_stopID and l_stop_info['seen']:
						instance_id+=1
					# while stops[(stopID, instance_id)].seen==10 and not was197:
					# 	instance_id+=1
					if (stopID,instance_id) in stops:
						stop = stops[(stopID, instance_id)]
						stop['bus'] = bus

						stop['avl_boards'] = stop['avl_boards']+boards
						stop['avl_alights'] = stop['avl_alights']+alights
						stop['avl_onboard'] = stop['avl_onboard']+onboard
						stop['boards'] = stop['boards']+boards
						stop['alights'] = stop['alights']+alights
						stop['onboard'] = stop['onboard']+onboard

						stop['actual_time'] = actual_time
						stop['seen']+= 1 if was197 else 10
						if not trip['actual_start'] or actual_time<trip['actual_start']:
							trip['actual_start'] = actual_time
						if not trip['actual_end'] or actual_time>trip['actual_end']:
							trip['actual_end'] = actual_time
	# def postProcess(self):
	# 	buses = {}
	# 	for date in self.root.keys():
	# 		for blockNumber in self.root[date].keys():
	# 			for tripNumber in self.root[date][blockNumber].keys():
	# 				for stopID in self.root[date][blockNunber][tripNumber]['stops'].keys():
	# 					stop = self.root[date][blockNunber][tripNumber]['stops'][stopID]
	# 					if stop['bus']==None:
	# 						pass
	# 						# Check day-bus-block actual times, take start and end time
	# 						# Check for overlap, if overlap, flag overlapping stops
	# 						# Mainly just find a mapping for bus
	# 					else
	# 						if stop['bus'] not in buses:
	# 							buses[stop['bus']] = []
	# 						buses[stop['bus']].append(stop)
	def flagDeviations(self):
		for date, day in self.root.items():
			for blockNumber, block in day.items():
				busesOnBlock = set()
				for tripNumber, trip in block.items():
					prevStop = None
					for stopID_instance, stop in trip['stops'].items():
						busesOnBlock.add(stop['bus'])
						if prevStop['bus']==stop['bus'] and stop['actual_time'] < prevStop['actual_time']:
							# We went back in time!
							stop['flag'] |= Flag.BACKWARDS_TIME
							prevStop['flag'] |= Flag.BACKWARDS_TIME
						prevStop = stop
				if len(busesOnBlock)>1:
					for trip in trip.values():
						trip['flag'] |= Flag.MULTIPLE_BUS_BLOCK

	def inferStops(self):
		# Organize data
		for date, day in self.root.items():
			for blockNumber, block in day.items():
				# busesOnBlock = {}
				# stopsToSort = {}
				prevStop = None
				for tripNumber, trip in block.items():
					lookAhead = []
					for stopID_instance, stop in trip['stops'].items():
						# look back
						if not stop['bus'] and prevStop:
							stop['bus'] = prevStop[1]['bus']
							stop['onboard'] = prevStop[1]['onboard']
						# look ahead
						else:
							lookAhead.append(stop)
						_, lastStop = list(trip['stops'].items())[len(trip['stops'])-1]
						_, sndToLastStop = list(trip['stops'].items())[len(trip['stops'])-2]
						# We determine that the end of the trip happened.
						tripCompleted = lastStop['seen'] and sndToLastStop['seen']
						if stop['seen'] and tripCompleted:
							while lookAhead:
								s = lookAhead.pop()
								s['bus'] = stop['bus']
								s['onboard'] = stop['onboard']
							# look ahead
						prevStop = (stopID_instance, stop)


	def getStopsByBusDict(self):
		stops_made = {}
		for date in self.root.keys():
			for blockNumber in self.root[date].keys():
				for tripNumber in self.root[date][blockNumber].keys():
					for stopID_instance in self.root[date][blockNunber][tripNumber]['stops'].keys():
						stop = self.root[date][blockNunber][tripNumber]['stops'][stopID_instance]
						if stop['bus'] in stops_made:
							stops_made[stop['bus']].append(stop)
						else:
							stop_made[stop['bus']] = [stop]
		return stops_made
	def getStopsByBus(self, bus):
		stops_made = []
		for date in self.root.keys():
			for blockNumber in self.root[date].keys():
				for tripNumber in self.root[date][blockNumber].keys():
					for stopID_instance in self.root[date][blockNunber][tripNumber]['stops'].keys():
						stop = self.root[date][blockNunber][tripNumber]['stops'][stopID_instance]
						if stop['bus'] == bus:
							stops_made.append(stop)
		return stops_made
