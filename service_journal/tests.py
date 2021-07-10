from unittest import TestCase, TestSuite
from .sql_handler import connection, config
from datetime import date

config.setup()

DATE_2020_1_30 = {

}

TEST_DATES = {
    date(2020, 1, 30): DATE_2020_1_30
}


class ConnectionReadTestCase(TestCase):
    def setUp(self):
        self.config = config.config

    def test_read_day(self):
        results = {}
        with connection.Connection(self.config) as connection_:
            for date_ in TEST_DATES:
                self.assertTrue(date_ not in results, f'{date} was added before we started date. Check queries for '
                                                      f'potential spillover of dates.')
                results[date_] = connection_.read(date_)
#
