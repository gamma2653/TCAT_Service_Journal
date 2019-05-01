#DBT_Classes = Day-Block-Trip classes
from abc import ABC, abstractmethod
# Abstract
class Deviations:
	def __init__(self):
		self.entries=[]
	def addEntry(self, e):
		if type(e)==type(()):
			self.entries.append(e)
		else:
			raise TypeError('addEntry got %s when it expected %s.' % (type(e), type(())))
	def removeEntry(self, e):
		self.entries.remove(e)

	def createDeviation(name, description, flag, block=None, trip=None, iStop=None, tStop=None, segment=None, bus=None, service_date=None):
		self.addEntry((name,description,flag,block,trip,iStop,tStop,segment,bus,service_date))

# Abstract Class
class Day(ABC):
	def __init__(self):
		self.date=date
	  	self.numberOfBlocks=0
	  	self.blocks=[]
		self.blockNumbers=[]

	# Mutable
	def getBlocks(self):
		return self.blocks
	def addBlock(self, b):
		self.blocks.append(b)
		self.numberOfBlocks+=1
		self.blockNumbers.append(b.getBlockNumber())
	def getBlock(self, blockNumber):
		for b in self.blocks:
			if b.blockNumber==blockNumber:
				return b
	def removeBlockNumber(self, blockNumber):
		b = self.getBlock(blockNumber)
		self.blocks.remove(b)
		self.blockNumbers.remove(blockNumber)
		self.numberOfBlocks-=1
# Concrete CLass
class ActualDay(Day):
	def __init__(self):
		super().__init__()
	  	self.busesUsed=[]
		# Used to store Deviation records
	  	self.deviations=Deviations()
	# Mutable
	def getBuses(self):
		return self.busesUsed
	def addBus(self, b):
		self.busesUsed.append(b)
	def getDeviations(self):
		return self.deviations
	def addDeviation(self, d):
		self.deviations.addEntry(d)
	def removeDeviation(self, d):
		self.deviations.removeEntry(d)
# Concrete Class
class ScheduledDay(Day):
	def __init__(self):
		super().__init__()

# Abstract Class
class Block(ABC):
	def __init__(self):
		self.blockNumber=blockNumber
		self.trips=[]
		self.numberOfTrips=0
	def setBlockNumber(self, bn):
		self.blockNumber=bn

	@abstractmethod
	def addTrip(self, t):
		pass
	# def removeTrip(self, t):
	# 	self.trips.remove(t)
class ActualBlock(Block):
	def __init__(self):
		super().__init__()
		self.numberOfBuses=0
		self.buses = []
	def addNewBuses(self, t): #Redo
		for s in t.getStops():
			if s.bus not in self.buses:
				self.buses.append(s.bus)
				self.numberOfBuses +=1
	def addTrip(self, t):
		self.trips.append(t)
		self.numberOfTrips+=1
		self.addNewBuses(self, t)
class ScheduledBlock(Block):
	def __init__(self):
		super().__init__()
	def addTrip(self, t):
		self.trips.append(t)
		self.numberOfTrips+=1

class Trip(ABC):
	def __init__(self, tripNumber, tripSeq, route, direction, pattern):
		self.stops=[]
		self.tripNumber = tripNumber
		self.tripSeq = tripSeq
		self.route = route
		self.direction = direction
		self.pattern = pattern
	def addStop(self, stop):
		pass
class ActualTrip(Trip):
	def __init__(self, tripNumber, tripSeq, route, direction, pattern, bus):
		super().__init__(tripNumber, tripSeq, route, direction, pattern)
		self.buses = [bus]
class ScheduledTrip(Trip):
	def __init__(self, tripNumber, tripSeq, route, direction, pattern):
		super().__init__(tripNumber, tripSeq, route, direction, pattern)
class Stop(ADT):
	def __init__(self, stopID, stopName, messageTypeID, stopTime, onboard, boards, alights):
		self.stopID = stopID
		self.stopName = stopName
		self.messageTypeID = messageTypeID
		self.stopTime = stopTime
		self.boards = boards
		self.alights = alights
