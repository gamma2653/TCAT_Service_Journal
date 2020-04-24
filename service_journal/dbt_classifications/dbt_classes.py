#DBT_Classes = Day-Block-Trip classes
from abc import ABC, abstractmethod
from enum import Enum
from collections import OrderedDict
from service_journal.dbt_classifications.exceptions import BlockNotFound, TripNotFound, NotActual, NotSchedule
from service_journal.gen_utils.debug import Logger
# Logger initialization
logger = Logger(__name__)
logger.read_args()

class Days:
	def __init__(self):
		self.root = dict()
	def addDay(self, date):
		self.root[date] = dict()
	def addBlock(self, date, blockNumber):
		if date not in self.root:
			self.addDay(self, date)
		self.root[date][blockNumber] = dict()
	def addTrip(self, date, blockNumber, tripNumber, route, direction):
		if date in self.root:
			self.addDay(self, date)
		if blockNumber not in self.root[date]:
			self.addBlock(self, date, blockNumber)
		self.root[date][blockNumber][tripNumber] = dict({'stops':OrderedDict(), route:route, direction:direction})
	# stopInfo is a tuple containing stopInfo
	def addStop(self, date, blockNumber, tripNumber, route, direction, stopID, stopName, time, distance,):
		if date in self.root:
			self.addDay(self, date)
		if blockNumber not in self.root[date]:
			self.addBlock(self, date, blockNumber)
		if tripNumber not in self.root[date][blockNumber]:
			self.addTrip(self, date, blockNumber, tripNumber)
		self.root[date][blockNumber][tripID]['stops'][stopID] = dict({'name':stopName,\
		 'time':time,'distance':distance, 'bus':None, 'boards':-1, 'alights':-1, \
		 'onboard':-1, 'adjustedOnboard':-1, 'seen':False})
	def crossRef(self, date, blockNumber, tripNumber, stopID, bus, boards, alights, onboard, adjustedOnboard):
		if date in self.root:
			if blockNumber in self.root[date]:
				if tripNumber in self.root[date][blockNumber]:
					if stopID in self.root[date][blockNumber][tripNumber]['stops']:
						stop = self.root[date][blockNumber][tripNumber]['stops'][stopID]
						stop['bus'] = bus
						stop['boards'] = boards
						stop['alights'] = alights
						stop['onboard'] = onboard
						stop['adjustedOnboard'] = adjustedOnboard
						stop['seen'] = True
						
