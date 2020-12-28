#DBT_Classes = Day-Block-Trip classes
import sys
from collections import OrderedDict
from service_journal.dbt_classifications.exceptions import BlockNotFound, TripNotFound, NotActual, NotSchedule
from service_journal.gen_utils.debug import Logger
from datetime import datetime, timedelta
from enum import IntFlag, Enum
# Logger initialization
logger = Logger(__name__)
logger.read_args()
class SortRules(Enum):
	"""
	An enum of sort rules.
	TODO: Replace usage with a sort function that gets passed.
	"""
	BY_BUS = 1
class Flag(IntFlag):
	"""
	A binary flag system used to flag deviations from the norm in a given stop
	or trip.
	"""
	FINE = 0
	BACKWARDS_TIME = 1
	MULTIPLE_BUS_BLOCK = 2

def closestStopID(stop_locations, loc):
	"""
	Paramters:
	[stop_locations] a dictionary with stop ids, "stop_id", as the key, and geographical
	stop locations, "stop_loc", as the value.
	"stop_loc" should be indexable by 0 and 1. {stop_loc[0]} should be longitude
	and {stop_loc[1]} should be latitude, as per convention.

	[loc] is the location we are trying to find the closest stop id to.
	"""
	min = sys.maxsize
	closestStop = 0
	for stop_id, stop_loc in stop_locations.items():
		dist = (float(stop_loc[0])-float(loc[0]))**2+(float(stop_loc[1])-float(loc[1]))**2
		if dist<=min:
			closestStop=stop_id
			min = dist
	return closestStop

class Journal:
	"""
	An class used to instantiate a journal. Should be able to accept multiple
	dates and store them all.

	NOTE: Currently bugged. Not able to store multiple dates for unknown reason.
	TODO: Fix this problem, or figure out if I was making a silly mistake when
	adding a new day.
	"""
	def __init__(self):
		self.root = dict()
	def addDay(self, date):
		self.root[date] = dict()
	def addBlock(self, date, blockNumber):
		if date not in self.root:
			self.addDay(date)
		self.root[date][blockNumber] = OrderedDict()
	def addTrip(self, date, blockNumber, tripNumber, route, direction):
		if date not in self.root:
			self.addDay(date)
		if blockNumber not in self.root[date]:
			self.addBlock(date, blockNumber)
		self.root[date][blockNumber][tripNumber] = {'stops':OrderedDict(), 'route':route, 'direction':direction, 'actual_start':None, 'actual_end':None, 'flag':Flag.FINE}
	# stopInfo is a tuple containing stopInfo
	def addStop(self, date, blockNumber, tripNumber, route, direction, stopID, stopName, sched_time, distance,run):
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
		# TODO: Why is this a while loop...?
		while (stopID, instance_id) in stops.keys():
			instance_id+=1
		stops[(stopID, instance_id)] = dict({'name':stopName,\
		 'sched_time':sched_time, 'actual_time':None,'distance':distance, 'bus':None,\
		 'avl_boards':0, 'avl_alights':0, 'avl_onboard':0, 'boards':0, 'alights':0, \
		 'onboard':0, 'adjustedOnboard':None, 'seen':0, 'run':run, 'flag':Flag.FINE})

	def fillIn(self, date, blockNumber, tripNumber, stopID, bus, boards, alights, onboard, actual_time, loc, route):
		if date in self.root:
			day = self.root[date]
			if blockNumber in day:
				block = day[blockNumber]
				if tripNumber in block:
					trip = block[tripNumber]
					stops = trip['stops']
					if stopID==0:
						if 'unorganized' not in stops:
							stops['unorganized'] = []
						stops['unorganized'].append((bus, board, alights, onboard, actual_time, loc, route))
					else:
						instance_id = 0
						# Identify loops
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
							stop['seen']=1
							if not trip['actual_start'] or actual_time<trip['actual_start']:
								trip['actual_start'] = actual_time
							if not trip['actual_end'] or actual_time>trip['actual_end']:
								trip['actual_end'] = actual_time

	def crossRef(self, date, blockNumber, tripNumber, stopID, bus, boards, alights, onboard, actual_time, loc, route, stop_locations):
		if date in self.root:
			day = self.root[date]
			if blockNumber in day:
				block = day[blockNumber]
				if tripNumber in block:
					trip = block[tripNumber]
					stops = trip['stops']
					# Infer stop
					if stopID==0:
						t_stop_locs = {stop_id : stop_locations[stop_id] for \
						(stop_id, instance_id) in stops.keys()}
						stopID = closestStopID(t_stop_locs, loc)
						was197 = True
					else:
						was197 = False
					instance_id = 0
					# Identify loops
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
	def sort(self, sort_rule=SortRules.BY_BUS):
		# Fairly innefficient but functional and easy way to insert into OrderedDict
		def insert(d, index, item):
			try:
				l = list(d.items())
			except AttributeError:
				l = list(d)
				l.insert(index, item)
				return OrderedDict(l)
		if sort_rule==SortRules.BY_BUS:
			for date, day in self.root.items():
				for blockNumber, block in day.items():
					oldTrips = dict(block)
					newTrips = OrderedDict()

					day[blockNumber] = newTrips

	# Post-processing engine
	# <processors> is a function of the format f(<instance>,[<arg1>,<arg2>,...],
	# [<kwarg1>, <kwarg2>,...]). <args> is a list of list of arguments.
	# <kwargs> is a list of list of keyword arguments. See post_process.py for
	# an example.
	def postProcess(self, processors, args=None, kwargs=None):
		# Ensure args exists and is long enough
		if not args:
			args = [None]*len(processors)
		if len(args)<len(processors):
			args.append(None)
		# Ensure kwargs exists and is long enough
		if not kwargs:
			kwargs = [(None,None)]*len(processors)
		if len(kwargs)<len(processors):
			args.append((None,None))
		# Run each post process passing self and arguments
		for i in range(0, len(processors)):
			processors[i](self, args=args[i], kwargs=kwargs[i])
# Example function for post_process
# def f(ins, args=[], kwargs=[]):
#   ins.a = args[0]
#   ins.b = args[1]
#   ins.kw = dict(kwargs)


# Would leave the instance with field "a" equalling first argument, b second
# argument, and the kw field equalling a dictionary of all passed kwargs.
	def flagDeviations(self):
		for date, day in self.root.items():
			for blockNumber, block in day.items():
				busesOnBlock = set()
				for tripNumber, trip in block.items():
					prevStop = None
					for stopID_instance, stop in trip['stops'].items():
						busesOnBlock.add(stop['bus'])
						# TODO: Replace the prevStop['actual_time']!=None with a mechanism to lookBack for the last available time. Maybe using a function
						if prevStop!=None and prevStop['bus']==stop['bus'] and stop['actual_time']!= None and prevStop['actual_time']!=None and stop['actual_time'] < prevStop['actual_time']:
							# We went back in time!
							stop['flag'] |= Flag.BACKWARDS_TIME
							prevStop['flag'] |= Flag.BACKWARDS_TIME
						prevStop = stop

				if len(busesOnBlock)>1:
					for trip in block.values():
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

	def __str__(self, indent=4, tabchar=' '):
		tab = ' '*indent if tabchar==' ' else '\t'
		print_out = '[Days]'
		for date, day in thing_with_dict.root.items():
			print_out+=f'{date}:\n'
			print_out+=f'{tab}[Blocks]\n'
			for blockNumber, block in day.items():
				print_out+=f'{tab}{blockNumber}:\n'
				print_out+=f'{tab*2}[Trips]\n'
				for tripNumber, trip in block.items():
					# Did it this disgusting way because I wanted stops printed last.
					print_out+=f'{tab*2}{tripNumber}:\n'
					print_out+=f'{tab*3}route: {trip["route"]}\n'
					print_out+=f'{tab*3}direction: {trip["direction"]}\n'
					print_out+=f'{tab*3}actual_start: {trip["actual_start"]}\n'
					print_out+=f'{tab*3}actual_end: {trip["actual_end"]}\n'
					print_out+=f'{tab*3}flag: {trip["flag"]}\n'
					print_out+=f'{tab*3}stops:\n'
					for (stop_id, stop_inst), stop in trip['stops'].items():
						print_out+=f'{tab*4}{stop_id} / {stop_inst}\n'
						for key, value in stop.items():
							print_out+=f'{tab*5}{key}: {value}\n'
		return print_out
