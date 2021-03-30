# DBT_Classes = Day-Block-Trip classes
from datetime import date
from typing import Iterable, Mapping

from service_journal.sql_handler.connection import Connection
from service_journal.gen_utils.debug import get_default_logger
logger = get_default_logger(__name__)


class Journal:
    """
    An class used to instantiate a journal. Should be able to accept multiple
    dates and store them all.

    NOTE: Currently bugged. Not able to store multiple dates for unknown reason.
    TODO: Fix this problem, or figure out if I was making a silly mistake when
    adding a new day.
    """

    def __init__(self, schedule: Mapping = None, avl_dict: Mapping = None, connection: Connection = None):
        self.schedule = {} if schedule is None else schedule
        self.avl_dict = {} if avl_dict is None else avl_dict
        self.connection = connection

    def update(self, schedule: Mapping = None, avl_dict: Mapping = None) -> None:
        """
        Updates the schedule and/or avl_dict with the given parameters.

        PARAMETERS
        --------
        schedule
            If given, any entries will be added/updated.
        avl_dict
            If given, any entries will be added/updated.
        """
        # TODO: Make sure it does a deep update
        if schedule is not None:
            self.schedule.update(schedule)
        if avl_dict is not None:
            self.avl_dict.update(avl_dict)

    def clear(self, schedule: bool = True, avl_dict: bool = True):
        if schedule:
            self.schedule.clear()
        if avl_dict:
            self.avl_dict.clear()

    def open(self, config: Mapping = None):
        self.connection = Connection(config)

    def close(self):
        self.connection.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def read_days(self, date_range: Iterable[date]) -> None:
        """
        Read date_range from a newly established connection.

        PARAMETERS
        --------
        date_range
            Iterable of date objects to load from the database.
        """
        with Connection() as conn:
            for day in date_range:
                self.schedule[day], self.avl_dict[day] = conn.read(day, format_=conn.DBT)

    def process(self) -> None:
        """
        Freshly processed the data in self.schedule and self.avl_dict and updates the schedule's internal book-keeping
        values.
        """
        for date_, day_actual in self.avl_dict.items():
            day_schedule = self.schedule[date_]

            for bus, bus_data in day_actual.items():
                for time_, report in bus_data.items():
                    try:
                        scheduled_stops = day_schedule[report['block_number']][report['trip_number']]['stops']
                        if report['stop_id'] == 0:
                            # Time to infer what happened! Magic time.
                            bus, trip, lat, lon = report['bus'], report['trip_number'], report['lat'], report['lon']

                        # We saw the stop, and know we got there via Avail
                        elif report['stop_id'] in scheduled_stops:
                            scheduled_stops[report['stop_id']]['seen'] += 1
                            scheduled_stops[report['stop_id']]['confidence_factors'].append(100)
                            # TODO: Check to see if going backwards
                            # day_schedule[report['block_number']][report['trip_number']]['seq_tracker'] =
                        else:
                            logger.warning('Stop not in schedule, what happened?\nStop_ID: %s', report['stop_id'])

                    except KeyError as e:
                        logger.error('Key does not exist in scheduled_stops. These are the keys:\n'
                                     'block_number=%s\ntrip_number=%s\nError:\n%s', report['block_number'],
                                     report['trip_number'], e)
                        logger.debug('day_schedule: %s', day_schedule)

    def post_process(self) -> None:
        """
        Updates internal book-keeping that could not be done on first sweep. This includes updating confidence values.
        """
        # Calculate confidence scores
        for date_, day_schedule in self.schedule.items():
            for block_number, block in day_schedule.items():
                for trip_number, trip in block.items():
                    stops = trip['stops']
                    for stop_id, stop in stops.items():
                        if stop['seen'] != 0:
                            stop['confidence_score'] = sum(stop['confidence_factors'])/stop['seen']
                        else:
                            stop['confidence_score'] = 0
    def write(self):
        pass

