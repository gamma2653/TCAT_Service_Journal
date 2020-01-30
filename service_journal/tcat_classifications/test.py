import unittest
from dbt_classes import ActualDay, ScheduledDay, ActualBlock, ScheduledBlock, ScheduledTrip, ActualTrip, ScheduledSegment, ActualSegment, Deviations, Exists
from datetime import date
from exceptions import *
# Test data
stopID0=0
stopID1=1
stopName0="Example 0"
stopName1="Example 1"
stopTime0 = 1000 # Example time. Only used as sql data so value in testing does
#                 not matter as much
stopTime1 = 2000 # Example time. Only used as sql data so value in testing does
#                 not matter as much
messageTypeID0 = 150
messageTypeID1 = 197
bus = 10
class TestActualSegment(unittest.TestCase):
	def test_cons(self):
		stop = ActualSegment(stopID0, stopID1, stopName0, stopName1, stopTime0, stopTime1, messageTypeID0, messageTypeID1, bus)
		self.assertEqual(stop.iStopID, stopID0)
		self.assertEqual(stop.tStopID, stopID1)
		self.assertEqual(stop.iStopName, stopName0)
		self.assertEqual(stop.tStopName, stopName1)
		self.assertEqual(stop.iStopTime, stopTime0)
		self.assertEqual(stop.tStopTime, stopTime1)
		self.assertEqual(stop.iMessageTypeID, messageTypeID0)
		self.assertEqual(stop.tMessageTypeID, messageTypeID1)
	def test_exists(self):
		stop = ActualSegment(stopID0, stopID1, stopName0, stopName1, stopTime0, stopTime1, messageTypeID0, messageTypeID1, bus)
		self.assertEqual(stop.get_exists(), Exists.ACTUAL)
class TestScheduledSegment(unittest.TestCase):
	def test_cons(self):
		stop = ScheduledSegment(stopID0, stopID1, stopName0, stopName1, stopTime0, stopTime1)
		self.assertEqual(stop.iStopID, stopID0)
		self.assertEqual(stop.tStopID, stopID1)
		self.assertEqual(stop.iStopName, stopName0)
		self.assertEqual(stop.tStopName, stopName1)
		self.assertEqual(stop.iStopTime, stopTime0)
		self.assertEqual(stop.tStopTime, stopTime1)
	def test_exists(self):
		stop = ScheduledSegment(stopID0, stopID1, stopName0, stopName1, stopTime0, stopTime1)
		self.assertEqual(stop.get_exists(), Exists.SCHEDULED)
# Test data
tripNumber = 140
tripSeq = 140
route = 60
direction = 'N'
aSegment = ActualSegment(stopID0, stopID1, stopName0, stopName1, stopTime0, stopTime1, messageTypeID0, messageTypeID1, bus)
sSegment = ScheduledSegment(stopID0, stopID1, stopName0, stopName1, stopTime0, stopTime1)

class TestActualTrip(unittest.TestCase):
	def test_cons(self):
		trip = ActualTrip(tripNumber, tripSeq, route, direction, bus, aSegment)
		self.assertEqual(trip.segments, [aSegment])
		self.assertEqual(trip.tripNumber, tripNumber)
		self.assertEqual(trip.tripSeq, tripSeq)
		self.assertEqual(trip.route, route)
		self.assertEqual(trip.direction, direction)
		buses = set()
		buses.add(bus)
		self.assertEqual(trip.buses, buses)

	def test_exists(self):
		trip = ActualTrip(tripNumber, tripSeq, route, direction, bus, aSegment)
		self.assertEqual(trip.get_exists(), Exists.ACTUAL)

	def test_add_stops(self):
		trip = ActualTrip(tripNumber, tripSeq, route, direction, bus, aSegment)
		expected = [aSegment]
		for i in range(1, 100):
			s = ActualSegment(i, i+1, ("Example %d" % (i)), ("Example %d" % (i+1)), 1000+i, 1001+i, 150, 150, bus)
			trip.addSegment(s)
			expected.append(s)
			# Test to make sure they are synonymous
			self.assertEqual(trip.segments, expected)
		self.assertRaises(NotActual, trip.addSegment, sSegment)

	def test_remove_stops(self):
		trip = ActualTrip(tripNumber, tripSeq, route, direction, bus, aSegment)
		seg2 = ActualSegment(stopID1, 10, stopName1, "Example 2", stopTime1, 1010, messageTypeID1, 150, bus)
		trip.addSegment(seg2)
		trip.removeSegment(10, initial=False)
		self.assertEqual(trip.segments, [aSegment])
		trip.removeSegment(aSegment)
		self.assertEqual(trip.segments, [])
		seg3 = ActualSegment(seg2.tStopID, 10, seg2.tStopName, "Example 3", seg2.tStopTime, 101010, seg2.tMessageTypeID, 150, bus)
		trip.addSegment(seg3)
		seg4 = ActualSegment(seg3.tStopID, 12, seg3.tStopName, "Example 4", seg3.tStopTime, 103213, seg3.tMessageTypeID, 150, bus)
		trip.addSegment(seg4)
		seg5 = ActualSegment(seg4.tStopID, 14, seg4.tStopName, "Example 5", seg4.tStopTime, 313131, seg4.tMessageTypeID, 150, bus)
		trip.addSegment(seg5)
		trip.removeSegment(12, initial=False)
		trip.removeSegment(seg3)
		self.assertEqual(trip.segments, [seg5])

class TestScheduledTrip(unittest.TestCase):
	def test_cons(self):
		trip = ScheduledTrip(tripNumber, tripSeq, route, direction, sSegment)
		self.assertEqual(trip.segments, [sSegment])
		self.assertEqual(trip.tripNumber, tripNumber)
		self.assertEqual(trip.tripSeq, tripSeq)
		self.assertEqual(trip.route, route)
		self.assertEqual(trip.direction, direction)
	def test_exists(self):
		trip = ScheduledTrip(tripNumber, tripSeq, route, direction, sSegment)
		self.assertEqual(trip.get_exists(), Exists.SCHEDULED)
	def test_add_stops(self):
		trip = ScheduledTrip(tripNumber, tripSeq, route, direction, sSegment)
		expected = [sSegment]
		for i in range(1, 100):
			s = ScheduledSegment(i, i+1, ("Example %d" % (i)), ("Example %d" % (i+1)), 1000+i, 1001+i)
			trip.addSegment(s)
			expected.append(s)
			# Test to make sure they are synonymous
			self.assertEqual(trip.segments, expected)
		self.assertRaises(NotSchedule, trip.addSegment, aSegment)
	def test_remove_stops(self):
		trip = ScheduledTrip(tripNumber, tripSeq, route, direction, sSegment)
		seg2 = ScheduledSegment(stopID1, 10, stopName1, "Example 2", stopTime1, 1010)
		trip.addSegment(seg2)
		trip.removeSegment(10, initial=False)
		self.assertEqual(trip.segments, [sSegment])
		trip.removeSegment(sSegment)
		self.assertEqual(trip.segments, [])
		seg3 = ScheduledSegment(seg2.tStopID, 10, seg2.tStopName, "Example 3", seg2.tStopTime, 101010)
		trip.addSegment(seg3)
		seg4 = ScheduledSegment(seg3.tStopID, 12, seg3.tStopName, "Example 4", seg3.tStopTime, 103213)
		trip.addSegment(seg4)
		seg5 = ScheduledSegment(seg4.tStopID, 14, seg4.tStopName, "Example 5", seg4.tStopTime, 313131)
		trip.addSegment(seg5)
		trip.removeSegment(12, initial=False)
		trip.removeSegment(seg3)
		self.assertEqual(trip.segments, [seg5])
# Test data
blockNumber = -1
class TestActualBlock(unittest.TestCase):
	def test_cons(self):
		block = ActualBlock(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [])
		self.assertEqual(block.numberOfTrips, 0)
		# Actual
		self.assertEqual(block.buses, set())
		self.assertEqual(block.numberOfBuses, 0)
	def test_setblocknumber(self):
		block = ActualBlock(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)
		block.setBlockNumber(10)
		self.assertEqual(block.blockNumber, 10)
		block.setBlockNumber(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)
	def test_addtrip(self):
		block = ActualBlock(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [])
		self.assertEqual(block.numberOfTrips, 0)
		self.assertEqual(block.buses, set())
		self.assertEqual(block.numberOfBuses, 0)
		trip = ActualTrip(tripNumber, tripSeq, route, direction, bus, aSegment)
		block.addTrip(trip)

		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [trip])
		self.assertEqual(block.numberOfTrips, 1)
		self.assertEqual(block.buses, {bus})
		self.assertEqual(block.numberOfBuses, 1)

		trip2 = ActualTrip(tripNumber+1, tripSeq+1, route+1, direction, bus+1, aSegment)
		block.addTrip(trip2)

		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [trip, trip2])
		self.assertEqual(block.numberOfTrips, 2)
		self.assertEqual(block.buses, set({bus, bus+1}))
		self.assertEqual(block.numberOfBuses, 2)

		trip3 = ActualTrip(tripNumber+2, tripSeq+2, route, direction, bus+1, aSegment)
		block.addTrip(trip3)

		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [trip, trip2, trip3])
		self.assertEqual(block.numberOfTrips, 3)
		self.assertEqual(block.buses, set({bus, bus+1}))
		self.assertEqual(block.numberOfBuses, 2)
	def test_gettrip(self):
		block = ActualBlock(blockNumber)
		trip = ActualTrip(tripNumber, tripSeq, route, direction, bus, aSegment)
		trip2 = ActualTrip(tripNumber+1, tripSeq+1, route+1, direction, bus+1, aSegment)
		trip3 = ActualTrip(tripNumber+2, tripSeq+2, route, direction, bus+1, aSegment)
		block.addTrip(trip)
		block.addTrip(trip2)
		block.addTrip(trip3)
		self.assertEqual(block.getTrip(tripNumber), trip)
		self.assertEqual(block.getTrip(tripNumber+1), trip2)
		self.assertEqual(block.getTrip(tripNumber+2), trip3)
class TestScheduledlBlock(unittest.TestCase):
	def test_cons(self):
		block = ScheduledBlock(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [])
		self.assertEqual(block.numberOfTrips, 0)

	def test_setblocknumber(self):
		block = ScheduledBlock(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)
		block.setBlockNumber(10)
		self.assertEqual(block.blockNumber, 10)
		block.setBlockNumber(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)

	def test_addtrip(self):
		block = ActualBlock(blockNumber)
		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [])
		self.assertEqual(block.numberOfTrips, 0)
		trip = ActualTrip(tripNumber, tripSeq, route, direction, bus, aSegment)
		block.addTrip(trip)

		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [trip])
		self.assertEqual(block.numberOfTrips, 1)

		trip2 = ActualTrip(tripNumber+1, tripSeq+1, route+1, direction, bus+1, aSegment)
		block.addTrip(trip2)

		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [trip, trip2])
		self.assertEqual(block.numberOfTrips, 2)

		trip3 = ActualTrip(tripNumber+2, tripSeq+2, route, direction, bus+1, aSegment)
		block.addTrip(trip3)

		self.assertEqual(block.blockNumber, blockNumber)
		self.assertEqual(block.trips, [trip, trip2, trip3])
		self.assertEqual(block.numberOfTrips, 3)

# Test data
ddate = date(2018, 8, 1)
class TestActualDay(unittest.TestCase):
	def test_cons(self):
		day = ActualDay(ddate)
		self.assertEqual(day.date, ddate)
		self.assertEqual(day.numberOfBlocks, 0)
		self.assertEqual(day.blocks, [])
		self.assertEqual(day.blockNumbers, set())
		self.assertEqual(day.busesUsed, [])
		self.assertEqual(day.deviations, Deviations())

class TestScheduledDay(unittest.TestCase):
	def test_cons(self):
		day = ScheduledDay(ddate)
		self.assertEqual(day.date, ddate)
		self.assertEqual(day.numberOfBlocks, 0)
		self.assertEqual(day.blocks, [])
		self.assertEqual(day.blockNumbers, set())


def run_tests():
	unittest.main()
if __name__=='__main__':
	print('\n\nRunning test suite...\n\n')
	run_tests()
else:
	print('No test run.')
