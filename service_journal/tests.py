from collections import OrderedDict
from unittest import TestCase, TestSuite, mock
from datetime import date, datetime
from .sql_handler import connection, config
import os
from copy import deepcopy

ENV_EDITS = {
    'JOURNAL_CONFIG_NAME': 'test_config.py',
    'JOURNAL_USE_CONFIG_FILE': False,
    'JOURNAL_FORCE_USE_CONFIG_FILE': False
}


with mock.patch.dict(os.environ, ENV_EDITS):
    config.setup()


TEST_DATES = {
    date(2020, 1, 30): {
        # Expected result of this test case
        # (scheduled, actuals)
        'test_read_day': (
            {
                '2020-01-30': {
                    100: {
                        430: {
                            'route': -1,
                            'stops': OrderedDict({
                                10005: {
                                    'sched_time': datetime(2020, 1, 30, 4, 30),
                                    'direction': 'D',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                            }),
                            'seq_tracker': 0
                        },
                        440: {
                            'route': 81,
                            'stops': OrderedDict({
                                1353: {
                                    'sched_time': datetime(2020, 1, 30, 4, 40),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1346: {
                                    'sched_time': datetime(2020, 1, 30, 4, 40, 44),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1344: {
                                    'sched_time': datetime(2020, 1, 30, 4, 41, 22),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1356: {
                                    'sched_time': datetime(2020, 1, 30, 4, 42, 20),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1326: {
                                    'sched_time': datetime(2020, 1, 30, 4, 43, 45),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1524: {
                                    'sched_time': datetime(2020, 1, 30, 4, 43, 45),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1534: {
                                    'sched_time': datetime(2020, 1, 30, 4, 44, 57),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1530: {
                                    'sched_time': datetime(2020, 1, 30, 4, 47, 13),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1776: {
                                    'sched_time': datetime(2020, 1, 30, 4, 48),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1531: {
                                    'sched_time': datetime(2020, 1, 30, 4, 48, 40),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1533: {
                                    'sched_time': datetime(2020, 1, 30, 4, 49, 25),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1541: {
                                    'sched_time': datetime(2020, 1, 30, 4, 49, 57),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1543: {
                                    'sched_time': datetime(2020, 1, 30, 4, 50, 28),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1545: {
                                    'sched_time': datetime(2020, 1, 30, 4, 51, 10),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1547: {
                                    'sched_time': datetime(2020, 1, 30, 4, 52),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                },
                                1555: {
                                    'sched_time': datetime(2020, 1, 30, 4, 54),
                                    'direction': 'SB',
                                    'seen': 0,
                                    'bus': None,
                                    'confidence_score': 0,
                                    'confidence_factors': [],
                                    'operator': None,
                                    'trigger_time': None,
                                    'boards': 0,
                                    'alights': 0,
                                    'onboard': 0,
                                    'name': None
                                }
                            }),
                            'seq_tracker': 0
                        },
                    },
                }
            },
            {
                '2020-01-30': {
                    1501: {
                        datetime(2020, 1, 30, 4, 30, 53): {
                            'lat': 42.452263,
                            'lon': -76.505157,
                            'dir': 'D',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 30, 42),
                            'boards': 0,
                            'alights': 0,
                            'onboard': 0,
                            'stop_id': 10005,
                            'name': None,
                            'block_number': 100,
                            'route': {999},
                            'trip_number': [430],
                        },
                        datetime(2020, 1, 30, 4, 40, 37): {
                            'lat': 42.458112,
                            'lon': -76.475458,
                            'dir': 'D',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 40, 37),
                            'boards': 26,
                            'alights': 0,
                            'onboard': 26,
                            'stop_id': 0,
                            'name': None,
                            'block_number': 100,
                            'route': {999},
                            'trip_number': [430],
                        },
                        datetime(2020, 1, 30, 4, 41, 44): {
                            'lat': 42.457995,
                            'lon': -76.478014,
                            'dir': 'D',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 41, 37),
                            'boards': 6,
                            'alights': 0,
                            'onboard': 32,
                            'stop_id': 1353,
                            'name': None,
                            'block_number': 100,
                            'route': {999, 81},
                            'trip_number': [430, 440],
                        },
                        datetime(2020, 1, 30, 4, 43, 12): {
                            'lat': 42.456741,
                            'lon': -76.475410,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 43, 12),
                            'boards': 0,
                            'alights': 0,
                            'onboard': 32,
                            'stop_id': 1346,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 43, 29): {
                            'lat': 42.456778,
                            'lon': -76.477601,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 43, 29),
                            'boards': 0,
                            'alights': 0,
                            'onboard': 32,
                            'stop_id': 1344,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 44, 19): {
                            'lat': 42.455779,
                            'lon': -76.481044,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 44, 13),
                            'boards': 0,
                            'alights': 0,
                            'onboard': 32,
                            'stop_id': 1356,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 45, 25): {
                            'lat': 42.452376,
                            'lon': -76.481169,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 45, 18),
                            'boards': 0,
                            'alights': 0,
                            'onboard': 32,
                            'stop_id': 1326,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 46): {
                            'lat': 42.451227,
                            'lon': -76.482399,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 46),
                            'boards': 0,
                            'alights': 2,
                            'onboard': 30,
                            'stop_id': 0,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 46, 53): {
                            'lat': 42.449004,
                            'lon': -76.482896,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 46, 46),
                            'boards': 0,
                            'alights': 7,
                            'onboard': 23,
                            'stop_id': 1524,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 47, 41): {
                            'lat': 42.446775,
                            'lon': -76.482725,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 47, 32),
                            'boards': 0,
                            'alights': 5,
                            'onboard': 18,
                            'stop_id': 1541,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 48, 21): {
                            'lat': 42.445489,
                            'lon': -76.482676,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 48, 21),
                            'boards': 0,
                            'alights': 7,
                            'onboard': 11,
                            'stop_id': 0,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 4, 49, 29): {
                            'lat': 42.443476,
                            'lon': -76.485351,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 49, 29),
                            'boards': 0,
                            'alights': 1,
                            'onboard': 10,
                            'stop_id': 0,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 4, 52, 50): {
                            'lat': 42.447566,
                            'lon': -76.477769,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 52, 38),
                            'boards': 0,
                            'alights': 4,
                            'onboard': 6,
                            'stop_id': 1543,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 4, 53, 34): {
                            'lat': 42.447578,
                            'lon': -76.474370,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 53, 23),
                            'boards': 0,
                            'alights': 3,
                            'onboard': 3,
                            'stop_id': 1545,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 4, 54, 2): {
                            'lat': 42.446756,
                            'lon': -76.471977,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 54, 2),
                            'boards': 0,
                            'alights': 0,
                            'onboard': 3,
                            'stop_id': 1547,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 4, 54, 38): {
                            'lat': 42.445664,
                            'lon': -76.471912,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 54, 38),
                            'boards': 0,
                            'alights': 3,
                            'onboard': 0,
                            'stop_id': 0,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                        datetime(2020, 1, 30, 4, 4, 56, 29): {
                            'lat': 42.447503,
                            'lon': -76.467522,
                            'dir': 'SB',
                            'operator': 61,
                            'depart': datetime(2020, 1, 30, 4, 56, 18),
                            'boards': 0,
                            'alights': 1,
                            'onboard': 0,
                            'stop_id': 1555,
                            'name': None,
                            'block_number': 100,
                            'route': {81},
                            'trip_number': [440],
                        },
                    },

                }
            }
        )
    }
}


def check_scheduled_equivalence(schedule, expected):
    for date_, blocks in expected.items():
        for block, trips in blocks.items():
            for trip, trip_data in trips.items():
                sched_trip = schedule[date_][block][trip]
                if sched_trip['seq_tracker'] != trip_data['seq_tracker'] or sched_trip['route'] != trip_data['route']:
                    return False
                # Check stop order
                expected_stops = list(trip_data['stops'].keys())
                scheduled_stops = list(sched_trip['stops'].keys())
                if expected_stops != scheduled_stops:
                    return False
                for stop_num, stop_data in trip_data['stops'].items():
                    for stop_datum_name, stop_datum_value in stop_data:
                        if stop_datum_value != sched_trip['stops'][stop_num][stop_datum_name]:
                            return False
    return True


def check_actuals_equivalence(actuals, expected):
    for date_, buses in expected.items():
        for bus, reports in buses.items():
            for time_, report in reports.items():
                for datum_name, datum_value in report.items():
                    if datum_value != actuals[date_][bus][time_][datum_name]:
                        return False
    return True


class ConnectionReadTestCase(TestCase):
    def setUp(self):
        self.config = config.config

    def test_read_day(self):
        fun_name = 'test_read_day'
        results = ({}, {})
        with connection.Connection(self.config) as conn:
            for date_, expected_results in TEST_DATES:
                self.assertTrue(date_ not in results, f'{date} was added before we started date. Check queries for '
                                                      f'potential spillover of dates.')
                read_ = conn.read(date_)
                results[0].update(read_[0])
                results[1].update(read_[1])
            for date_, date_results in TEST_DATES:
                self.assertTrue(
                    check_scheduled_equivalence(results[0], date_results[fun_name][0])
                )
                self.assertTrue(
                    check_actuals_equivalence(results[1], date_results[fun_name][1])
                )


class ConnectionWriteTestCase(TestCase):
    def test_write_day(self):
        pass


class JournalProcessTest(TestCase):
    def test_one_process(self):
        pass

    def test_multiple_process(self):
        pass

    def test_multiprocessing(self):
        pass
