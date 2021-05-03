# DBT_Classes = Day-Block-Trip classes
from datetime import date
from typing import Iterable, Mapping

from service_journal.classifications.exceptions import PreconditionError
from service_journal.sql_handler.connection import Connection, DataFormat
from service_journal.gen_utils.debug import get_default_logger
from service_journal.gen_utils.class_utils import date_range, get_deflt_processors, DEFAULT_PROCESSOR_TYPES

logger = get_default_logger(__name__)


class Journal:
    """
    A class used to instantiate a journal. Should be able to accept multiple
    dates and store them all.

    NOTE: Currently bugged. Not able to store multiple dates for unknown reason.
    TODO: Fix this problem, or figure out if I was making a silly mistake when
    adding a new day.
    """

    def __init__(self, schedule: Mapping = None, avl_dict: Mapping = None, stop_locations: Mapping = None,
                 shapes: Mapping = None, intervals_not_visited: Mapping = None, connection: Connection = None,
                 config: Mapping = None):
        self.schedule = {} if schedule is None else schedule
        self.avl_dict = {} if avl_dict is None else avl_dict
        self.stop_locations = {} if stop_locations is None else stop_locations
        self.shapes = {} if shapes is None else shapes
        self.intervals_not_visited = {} if intervals_not_visited is None else intervals_not_visited
        self.connection = connection
        self.config = config
        self.processors = get_deflt_processors()

    def _raise_if_not_open(self):
        if not self.connection.is_open:
            raise PreconditionError('To use this method, the journal\'s connection must be open.')

    def update(self, schedule: Mapping = None, avl_dict: Mapping = None, stop_locations: Mapping = None,
               shapes: Mapping = None, intervals_not_visited: Mapping = None):
        """
        Updates the schedule and/or avl_dict with the given parameters.
        """
        # TODO: Make sure it does a deep update
        if schedule is not None:
            self.schedule.update(schedule)
        if avl_dict is not None:
            self.avl_dict.update(avl_dict)
        if stop_locations is not None:
            self.stop_locations.update(stop_locations)
        if shapes is not None:
            self.shapes.update(shapes)
        if intervals_not_visited is not None:
            self.intervals_not_visited.update(intervals_not_visited)

    def clear(self, schedule: bool = True, avl_dict: bool = True, intervals_not_visited: bool = True,
              shapes: bool = False, stop_locations: bool = False):
        """
        Clears out the internal dictionaries. By default clears the data that is date dependent.
        """
        if schedule:
            self.schedule.clear()
        if avl_dict:
            self.avl_dict.clear()
        if intervals_not_visited:
            self.intervals_not_visited.clear()
        if stop_locations:
            self.stop_locations.clear()
        if shapes:
            self.shapes.clear()

    def open(self, config: Mapping = None):
        config = config if config is not None else self.config
        self.close()
        self.connection = Connection(config)
        self.connection.open()

    def close(self):
        if self.connection is not None:
            if self.connection.is_open:
                self.connection.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def read_stops_locations(self):
        self._raise_if_not_open()
        self.update(stop_locations=self.connection.load_stop_loc())

    def read_shapes(self):
        self._raise_if_not_open()
        self.update(shapes=self.connection.load_shapes())

    def read_day_independent(self):
        self.read_stops_locations()
        self.read_shapes()

    def read_day(self, day):
        self._raise_if_not_open()
        self.update(*self.connection.read(day, format_=DataFormat.DBT))

    def read_days(self, date_range_: Iterable[date]):
        """
        Read date_range from a newly established connection.

        PARAMETERS
        --------
        date_range_
            Iterable of date objects to load from the database.
        """
        self._raise_if_not_open()
        for day in date_range_:
            self.update(*self.connection.read(day, format_=DataFormat.DBT))

    def add_processor(self, type_, processor):
        if type_ not in self.processors:
            self.processors[type_] = []
        self.processors[type_].append(processor)

    def clear_processors(self, type_=None):
        if type_ is None:
            self.processors = get_deflt_processors()
        else:
            self.processors[type_].clear()

    def install_processor_preset(self, processors):
        self.processors = processors

    def process(self, type_):
        for processor in self.processors[type_]:
            processor(self)

    def process_all(self, types_=DEFAULT_PROCESSOR_TYPES):
        """
        Convenience method for running preprocess, process, and post_process in that order.

        Runs processors according to the parameters (all by default).

        PARAMETERS
        --------
        types_
            Types of prep to run. 'prep', 'main', and 'post' by default.
        """
        for type_ in types_:
            self.process(type_)

    def write(self):
        """
        Writes the stored schedule and avl_dict to the output database. Requires an open connection.
        """
        self._raise_if_not_open()

        logger.info('Beginning to write data.')
        for date_, day_schedule in self.schedule.items():
            for block_number, block in day_schedule.items():
                for trip_number, trip in block.items():
                    stops = trip['stops']
                    for stop_id, stop in stops.items():
                        self.connection.write({
                            'date': date_,
                            'bus': stop['bus'],
                            'report_time': stop['trigger_time'],
                            'dir': stop['direction'],
                            'route': trip['route'],
                            'block_number': block_number,
                            'trip_number': trip_number,
                            'operator': stop['operator'],
                            'boards': stop['boards'],
                            'alights': stop['alights'],
                            'onboard': stop['onboard'],
                            'stop': stop_id,
                            'stop_name': stop['name'],
                            'sched_time': stop['sched_time'],
                            'seen': stop['seen'],
                            'confidence_score': stop['confidence_score']
                        }, autocommit=False)
        self.connection.commit()
        logger.info('Finished writing data.')

    def process_dates_batch(self, from_date, to_date, hold_data: bool = False, types_=DEFAULT_PROCESSOR_TYPES):
        self.read_day_independent()
        if hold_data:
            self.read_days(date_range_=date_range(from_date, to_date))
            self.process_all(types_=types_)
            self.write()
        else:
            for day in date_range(from_date, to_date):
                self.clear()
                self.read_day(day)
                self.process_all(types_=types_)
                self.write()

