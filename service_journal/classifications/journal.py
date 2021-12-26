# DBT_Classes = Day-Block-Trip classes
from datetime import date
from typing import Iterable, Mapping, Optional, List

from .exceptions import PreconditionError
from .processors import get_deflt_processors
from ..sql_handler.connection import Connection
from ..utilities.debug import get_default_logger
from ..utilities.utils import date_range

logger = get_default_logger(__name__)


class Journal:
    """
    A class used to instantiate a journal. Should be able to accept multiple
    dates and store them all.

    NOTE: Currently bugged. Not able to store multiple dates for unknown reason.
    TODO: Fix this problem, or figure out if I was making a silly mistake when
    adding a new day.
    """
    def __init__(self, config: Optional[Mapping] = None, schedule: Optional[Mapping] = None, avl_dict: Optional[Mapping] = None,
                 stop_locations: Optional[Mapping] = None, shapes: Optional[Mapping] = None,
                 connection: Optional[Connection] = None, processors: Optional[Mapping[str, List]] = None):
        self.schedule = {} if schedule is None else schedule
        self.avl_dict = {} if avl_dict is None else avl_dict
        self.stop_locations = {} if stop_locations is None else stop_locations
        self.shapes = {} if shapes is None else shapes
        self.connection = connection
        self.config = config
        self.processors = get_deflt_processors() if processors is None else processors

    def _raise_if_not_open(self):
        if not self.connection.is_open:
            raise PreconditionError('To use this method, the journal\'s connection must be open.')

    def update(self, schedule: Mapping = None, avl_dict: Mapping = None, stop_locations: Mapping = None,
               shapes: Mapping = None):
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

    def clear(self, schedule: bool = True, avl_dict: bool = True, stop_locations: bool = False,
              shapes: bool = False):
        """
        Clears out the internal dictionaries. By default clears the data that is date dependent.
        """
        if schedule:
            self.schedule.clear()
        if avl_dict:
            self.avl_dict.clear()
        if stop_locations:
            self.stop_locations.clear()
        if shapes:
            self.shapes.clear()

    def open(self, config: Mapping = None):
        self.config = config if config is not None else self.config
        self.close()
        self.connection = Connection(self.config)
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
        logger.debug('Reading stop_locations.')
        self._raise_if_not_open()
        self.update(stop_locations=self.connection.load_stop_loc())

    def read_shapes(self):
        logger.debug('Reading shapes.')
        self._raise_if_not_open()
        self.update(shapes=self.connection.load_shapes())

    def read_day_independent(self):
        logger.debug('Reading data that is independent from date.')
        self.read_stops_locations()
        self.read_shapes()
        logger.debug('Done reading data that is independent from date.')

    def read_day(self, day, block=None):
        logger.debug('Reading [%s] from connection.', day)
        self._raise_if_not_open()
        if block is None:
            self.update(*self.connection.read(day))
        else:
            self.update(*self.connection.read(day, type_='alternate', params=[block]))

    def read_days(self, date_range_: Iterable[date], block: Optional[int] = None):
        """
        Read date_range from a newly established connection.

        PARAMETERS
        --------
        date_range_
            Iterable of date objects to load from the database.
        block
            Block to read. Defaults to all blocks
        """
        logger.debug('Initiating read_days for date_range: %s for block: %s', date_range, block)
        self._raise_if_not_open()
        if block is None:
            for day in date_range_:
                logger.info('Reading in data for: %s', day)
                self.update(*self.connection.read(day))
        else:
            for day in date_range_:
                logger.info('Reading in data for: %s for block: %s', day, block)
                self.update(*self.connection.read(day, type_='alternate', params=[block]))

    def add_processor(self, type_, processor):
        logger.debug('Adding %s to the %s layer of processors.', processor, type_)
        if type_ not in self.processors:
            self.processors[type_] = []
        self.processors[type_].append(processor)
        logger.debug('Processor loaded.')

    def clear_processors(self, type_=None):
        logger.debug('Clearing processors.')
        if type_ is None:
            self.processors = get_deflt_processors()
        else:
            self.processors[type_].clear()
        logger.debug('All processors cleared.')

    def install_processor_preset(self, processors):
        logger.debug('Installing processor preset.')
        self.processors = processors
        logger.debug('Processor preset installed.')

    def process(self, type_):
        logger.debug('Processing layer: %s', type_)
        for processor in self.processors[type_]:
            logger.debug('Running processor: %s', processor)
            processor(self)

    def process_all(self, types_: Optional[Iterable[str]] = None):
        """
        Convenience method for running preprocess, process, and post_process in that order.

        Runs processors according to the parameters (all by default).

        PARAMETERS
        --------
        types_
            Iterable of strings that are keys to installed processors. eg) defaults are prep, main, post. Defaults to
            all installed processors.
        """
        logger.debug('Processing all processors: %s', self.processors)
        types_ = self.processors.keys() if types_ is None else types_
        for type_ in types_:
            self.process(type_)

    def write(self):
        """
        Writes the stored schedule and avl_dict to the output database. Requires an open connection.
        """
        self._raise_if_not_open()
        # TODO: Use "packager" system in this writing
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

    def process_dates_batch(self, from_date, to_date, hold_data: bool = False, types_: Optional[Iterable[str]] = None,
                            block: Optional[int] = None):
        types_ = self.processors.keys() if types_ is None else types_
        logger.info('Processing a batch of days from %s to %s. hold_data=%s types_=%s block=%s', from_date, to_date,
                    hold_data, types_, block)
        self.read_day_independent()
        logger.debug('Day independent data has been loaded.')
        if hold_data:
            self.read_days(date_range_=date_range(from_date, to_date), block=block)
            self.process_all(types_=types_)
            self.write()
        else:
            for day in date_range(from_date, to_date):
                self.clear()
                self.read_day(day, block=block)
                self.process_all(types_=types_)
                self.write()
