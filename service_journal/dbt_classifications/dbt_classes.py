#DBT_Classes = Day-Block-Trip classes
from abc import ABC, abstractmethod
from enum import Enum
from service_journal.dbt_classifications.exceptions import BlockNotFound, TripNotFound, NotActual, NotSchedule
from service_journal.gen_utils.debug import Logger
# Logger initialization
logger = Logger(__name__)
logger.read_args()


class Stop():
	def __init__(self, blockNumber, seq, stopID, stopName, time, distance, seen=False):
		self.parent = None
		self.blockNumber = blockNumber
		self.seq = seq
		self.stopID = stopID
		self.stopName = stopName
		self.time = time
		self.distance = distance
		# Keeping this for geo checks
		self.seen = seen


# [tripNumber] is an int representing the trip's ID number.
# [tripSeq] is an int represetning the trip's sequence number.
# [route] is an int representing the the trip's route.
# [direction] is a str representing the trip's direction.
# [stops] is a list of Stop, which is the first stop of the trip.
class Trip():
	def __init__(self, blockNumbers, tripNumber, tripSeq, route, direction, stops=[]):
		self.parents = set()
		self.blockNumber = blockNumber
		self.stops = stops
		self.tripNumber = tripNumber
		self.tripSeq = tripSeq
		self.route = route
		self.direction = direction

	def addStop(self, s):
		s.parents.add(self)
		self.stops.append(s)
		# Add check later
		self.blockNumber= s.blockNumber

	def get_stopIDs(self):
		return [stop.stopID for stop in self.stops]

	# Useful for adding a stop before [index:] number of stops.
	def increment_seq(self, index = 0, amount=1):
		working = self.stops[index:]
		for stop in working:
			stop.seq+=amount

	# [s] can be either a Stop object or a StopID (int). if [correct_meta],
	# function will add distance from removed stop to previous stop, and update
	# the seq field. Returns None if stop was not resolved.
	def removeStop(self, s, correct_meta=False):
		try:
			index = self.stops.index(s)
		except ValueError:
			try:
				index = self.get_stopIDs().index(s)
			except ValueError:
				return None

		if correct_meta:
			self.increment_seq(index = index, amount = -1)
			if index>0:
				self.stops[index-1].distance_feet+= self.stops[index].distance_feet
				self.stops[index-1].mileage+= self.stops[index].mileage
		return self.stops.pop(index)
		# Not recommended to use stopIDs in case of trip loop, but it won't break



class BB():
	def __init__(self):
		self.parent = None
		self.trips=[]

	def addTrip(self, t):
		if t.parent!=self:
			logger.warn('Reassigning parent. Old parent = %s, new parent = %s.'\
			% (str(t.parent), str(self)))
		t.parent = self
		self.trips.append(t)

	def getTrip(self, tn):
		for t in self.trips:
			if tn==t.tripNumber:
				return t
		raise TripNotFound('Trip (%s) was not found in %s.' % (tn, self.trips))

	def removeTrip(self, t):
		self.trips.remove(t)

	def getTripNumbers(self):
		# TODO: Test mutability later
		return [t.tripNumber for t in self.trips]


class Bus(BB):
	def __init__(self, busNumber, blockNumbers=set()):
		super().__init__()
		self.busNumber=busNumber
		self.blockNumbers = blockNumbers

	def addTrip(self, t):
		super().addTrip(t)
		for b in t.blockNumbers:
			self.blockNumbers.add(b)

	# Removes and re-adds all the blockNumbers.
	def validateBlockNumbers(self):
		self.blockNumbers = set()
		for t in self.trips:
			self.blockNumbers.add(t.blockNumber)
	# Example:
	# trip1 = block(1) trip2 = block(3) trip3 = block(1) and remove trip3
	# If you don't validate, it'll remove block(1)
	def removeTrip(self, t):
		super().removeTrip(t)
		self.validateBlockNumbers()


class Block(BB):
	def __init__(self, blockNumber):
		self.__init__()
		self.blockNumber=blockNumber


class Day():
	def __init__(self, date):
		# TODO: Maybe add a dictionary lookup for buses and blocks?
		self.date=date
		self.blocks=set()
		self.buses=set()

	def addBlock(self, b):
		if b.parent!=self:
			logger.warn('Reassigning parent. Old parent = %s, new parent = %s.'\
			% (str(b.parent), str(self)))
		b.parent = self
		self.blocks.add(b)
	def getBlock(self, blockNumber):
		for b in self.blocks:
			if b.blockNumber==blockNumber:
				return b
		raise BlockNotFound

	def removeBlock(self, blockNumber):
		b = self.getBlock(blockNumber)
		self.blocks.remove(b)

	def addBus(self, b):
		if b.parent!=self:
			logger.warn('Reassigning parent. Old parent = %s, new parent = %s.'\
			% (str(b.parent), str(self)))
		b.parent = self
		self.buses.add(b)

	def getBus(self, busNumber):
		for b in self.buses:
			if b.busNumber==busNumber:
				return b
		raise BusNotFound

	def removeBus(self, busNumber):
		b = self.getBus(busNumber)
		self.buses.remove(b)

	def getTripNumbers(self, actual=True):
		trips = set()
		# Some Python conditional expression magic, read like English
		for b in (self.buses if actual else self.blocks):
			trips.union(b.getTripNumbers())
	def getBlockNumbers(self, actual=True):
		blocks = set()
		if actual:
			for b in self.buses:
				blocks.union(b.blockNumbers)
			return blocks
		else:
			for b in self.blocks:
				blocks.add(b.blockNumber)
	def getBusNumbers(self):
		buses = set()
		for b in self.buses:
			buses.add(b.busNumber)
		return buses
	# Searches through blocks for scheduled trip.
	def resolveScheduledTrip(self, tn):
		trips = set()
		for b in self.blocks:
			try:
				trips.add(b.getTrip(tn))
			except TripNotFound:
				pass
		return trips
