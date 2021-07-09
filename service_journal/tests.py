from unittest import TestCase, TestSuite
from service_journal.sql_handler import connection, environ


class ConnectionReadTestCase(TestCase):
    def setUp(self):
        environ.setup()
        self.config = environ.config
        self.connection = connection.Connection()
