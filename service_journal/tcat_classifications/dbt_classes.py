#DBT_Classes = Day-Block-Trip classes
from abc import ABC, abstractmethod
from enum import Enum
from .exceptions import BlockNotFound, TripNotFound, NotActual, NotSchedule

# # Deviations are used as sql entries.
# # [entries] are a list of tuples of the type (str,str,str,Block,Trip,Stop,Stop,Segment,int,Date)
# class Deviations:
# 	def __init__(self):
# 		self.entries=[]
# 	# Adds the tuple to the [entries] buffer.
# 	def addEntry(self, e):
# 		if type(e)==type(()):
# 			self.entries.append(e)
# 		else:
# 			raise TypeError('addEntry got %s when it expected %s.' % (type(e), type(())))
# 	# Removes the tuple from the [entries] buffer
# 	def removeEntry(self, e):
# 		self.entries.remove(e)
#
# 	# Using tuples to always ensure at least None for keywords.
# 	# creates the tuple and adds it to [entries]
# 	def createDeviation(self, name, description, flag, block=None, trip=None, iStop=None, tStop=None, segment=None, bus=None, service_date=None):
# 		self.addEntry((name,description,flag,block,trip,iStop,tStop,segment,bus,service_date))
# 	# Comparisons are done via comparing self.entries to y.entries.
# 	def __eq__(self, y):
# 		return getattr(self, "entries", None) == getattr(y, "entries", None)
# 		# getattr to catch situation where entries are None if ever occurs.
#

# [stopID] is the id of the stop
# [stopName] is a string representation of the stop's name
# [stopTime] is a datetime object representing the time the stop was visited
# 	or was scheduled for.
class Segment():
	def __init__(self, iStopID, tStopID, iStopName, tStopName, iStopTime, tStopTime, seq, start, end):
		self.iStopID = iStopID
		self.tStopID = tStopID
		self.iStopName = iStopName
		self.tStopName = tStopName
		self.iStopTime = iStopTime
		self.tStopTime = tStopTime
		# self.iMessageTypeID = iMessageTypeID
		# self.tMessageTypeID = tMessageTypeID
		self.segSeq=seq
		self.start=start
		self.end=end
# [tripNumber] is an int representing the trip's ID number.
# [tripSeq] is an int represetning the trip's sequence number.
# [route] is an int representing the the trip's route.
# [direction] is a str representing the trip's direction.
# [stops] is a list of Stop, which is the first stop of the trip.
class Trip():
	def __init__(self, tripNumber, tripSeq, route, direction, segment):
		self.segments=[segment]
		self.tripNumber = tripNumber
		self.tripSeq = tripSeq
		self.route = route
		self.direction = direction
	def addSegment(self, s):
		self.segments.append(s)
	def removeSegment(self, s, initial=True):
		return self.__remove__(s, initial)
	# Built in method to remove a stop from the trip.
	# Attempts to use [s] as if it is a Segment to remove from the list.
	# If this fails, treats [s] as a stopID
	# Does not perform
	# 	proper operations for specific types of trip.
	def __remove__(self, s, initial = True):
		try:
			self.segments.remove(s)
			return True
		except ValueError:
			for seg in self.segments:
				if (initial and seg.iStopID==s) or ( (not initial) and seg.tStopID==s):
					self.segments.remove(seg)
					return True
		return False

# Abstract Class
# [blockNumber] is an int that represents the block's number
# [trips] is a list of type Trip, which represents the trips taken in the block.
# [numberOfTrips] is an int counting the number of trips.
class BB():
	def __init__(self):
		self.trips=[]
	def addTrip(self, t):
		self.trips.append(t)
	def getTrip(self, tn):
		for t in self.trips:
			if tn==t.tripNumber:
				return t
		raise TripNotFound('Trip (%s) was not found in %s.' % (tn, self.trips))
	def removeTrip(self, t):
		self.trips.remove(t)

class Bus(BB):
	def __init__(self, busNumber):
		self.__init__()
		self.busNumber=busNumber
class Block(BB):
	def __init__(self, blockNumber):
		self.__init__()
		self.blockNumber=blockNumber
class Day(DBT):
	def __init__(self, date):
		self.date=date
		self.blocks=[]
		self.buses=[]
		self.blockNumbers=set()
		self.busNumbers=set()
	def addBlock(self, b):
		self.blocks.add(b)
		self.blockNumbers.append(b.blockNumber)
	def getBlock(self, blockNumber):
		for b in self.blocks:
			if b.blockNumber==blockNumber:
				return b
		raise BlockNotFound
	def removeBlock(self, blockNumber):
		b = self.getBlock(blockNumber)
		self.blocks.remove(b)
		if self.blocks.count(b)==0:
			self.blockNumbers.remove(blockNumber)
	def addBus(self, b):
		self.buses.add(b)
		self.busNumbers.append(b.busNumber)
	def getBus(self, busNumber):
		for b in self.buses:
			if b.busNumber==busNumber:
				return b
		raise BusNotFound
	def removeBus(self, busNumber):
		b = self.getBus(busNumber)
		self.buses.remove(b)
		if self.buses.count(b)==0:
			self.busNumbers.remove(busNumber)
	# def getDeviations(self):
	# 	return self.deviations
	# def addDeviation(self, d):
		# self.deviations.addEntry(d)
	# def removeDeviation(self, d):
	# 	self.deviations.removeEntry(d)
