# DBT_Classes = Day-Block-Trip classes
from datetime import date
from typing import Iterable, Mapping

from service_journal.classifications.exceptions import PreconditionError
from service_journal.command_line_tools import date_range
from service_journal.sql_handler.connection import Connection, DataFormat
from service_journal.gen_utils.debug import get_default_logger
from service_journal.gen_utils.class_utils import reorganize_map, DATE_BLOCK_TRIP, DATE_BUS_TIME

from detour_analyzer.trip_analyzer.segments import track_intervals
from detour_analyzer.trip_analyzer.data_processing import expand_shape_dict

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
        # Print one value to know types
        for key, value in self.shapes.items():
            print(value)
            print(type(value[0]), type(value[1]))
            break

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

    def preprocess(self):
        """
        Code to be run before the primary processing of data. This includes running the data through Jonathan's code
        so that those results can be pulled from when compiling the final service journal.
        """
        # TODO: 1. Get shapes 2. Expand them 3. Convert actuals to Date-Block-Trip 4. Call track_intervals
        converted_actuals = reorganize_map[DATE_BUS_TIME][DATE_BLOCK_TRIP](self.avl_dict)
        expanded_shapes = expand_shape_dict(self.shapes)
        self.intervals_not_visited = track_intervals(expanded_shapes, self.stop_locations, converted_actuals)

    def process(self):
        """
        Freshly processed the data in self.schedule and self.avl_dict and updates the schedule's internal book-keeping
        values.
        """
        logger.debug('Processing.\nSchedule: %s\nActuals: %s', self.schedule, self.avl_dict)
        for date_, day_actual in self.avl_dict.items():
            logger.debug('Getting info for: %s', date_)
            day_schedule = self.schedule[date_]

            for bus, bus_data in day_actual.items():
                for time_, report in bus_data.items():
                    try:
                        scheduled_stops = day_schedule[report['block_number']][report['trip_number']]['stops']
                        if report['stop_id'] == 0:
                            pass
                            # Time to infer what happened! Magic time.
                            # trip, lat, lon = report['trip_number'], report['lat'], report['lon']

                        # We saw the stop, and know we got there via Avail
                        elif report['stop_id'] in scheduled_stops:
                            stop_id = report['stop_id']
                            scheduled_stops[stop_id]['seen'] += 1
                            scheduled_stops[stop_id]['confidence_factors'].append(100)
                            scheduled_stops[stop_id]['trigger_time'] = time_
                            scheduled_stops[stop_id]['operator'] = report['operator']
                            scheduled_stops[stop_id]['boards'] += report['boards']
                            scheduled_stops[stop_id]['alights'] += report['alights']
                            original_onboard = scheduled_stops[stop_id]['onboard']
                            scheduled_stops[stop_id]['onboard'] = max(report['onboard'], original_onboard)
                            scheduled_stops[stop_id]['bus'] = bus

                            # TODO: Check to see if going backwards
                            # day_schedule[report['block_number']][report['trip_number']]['seq_tracker'] =
                        else:
                            logger.warning('Stop not in schedule, what happened?\nStop_ID: %s\nBlock: %s\nTrip: %s\n'
                                           'Day: %s', report['stop_id'], report['block_number'], report['trip_number'],
                                           date_)

                    except KeyError as e:
                        logger.error('Key does not exist in scheduled_stops. These are the keys:\n'
                                     'block_number=%s\ntrip_number=%s\nKeys in report: %s\nError:\n%s',
                                     report['block_number'], report['trip_number'], report.keys(), e)
                        logger.debug('day_schedule: %s', day_schedule)

    def post_process(self):
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
                            stop['confidence_score'] = sum(stop['confidence_factors']) / stop['seen']
                        else:
                            stop['confidence_score'] = 0

    def process_all(self, preprocess: bool = True, process: bool = True, post_process: bool = True):
        """
        Convenience method for running preprocess, process, and post_process in that order.

        Runs preprocess, process, and post_process according to the parameters (all True by default).

        PARAMETERS
        --------
        preprocess
            Runs the preprocess method.
        process
            Runs the process method.
        post_process
            Runs the post_process method.
        """
        if preprocess:
            self.preprocess()
        if process:
            self.process()
        if post_process:
            self.post_process()

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

    def process_dates_batch(self, from_date, to_date, hold_data: bool = False, post_process: bool = True):
        self.read_day_independent()
        if hold_data:
            self.read_days(date_range_=date_range(from_date, to_date))
            self.process_all(post_process=post_process)
            self.write()
        else:
            for day in date_range(from_date, to_date):
                self.clear()
                self.read_day(day)
                self.process_all(post_process=post_process)
                self.write()

