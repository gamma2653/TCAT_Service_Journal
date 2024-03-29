from typing import TYPE_CHECKING

from gamlogger import get_default_logger

# Dynamic local imports
try:
    from ..utilities.utils import get_shape_trip, get_distance_on_segment_from_report, \
        get_trip_progress, replace_if_default, get_segment_length
except ImportError:
    from service_journal.utilities.utils import get_shape_trip, get_distance_on_segment_from_report, \
        get_trip_progress, replace_if_default, get_segment_length


if TYPE_CHECKING:
    try:
        from .journal import Journal
    except ImportError:
        from service_journal.classifications.journal import Journal

logger = get_default_logger(__name__)


def build_trip_shapes(journal: 'Journal'):
    journal.trip_shapes = {}


def process_take1(journal: 'Journal'):
    """
    Freshly processed the data in self.schedule and self.avl_dict and updates the schedule's internal book-keeping
    values.
    """
    # logger.debug('Processing.\nSchedule: %s\nActuals: %s', journal.schedule, journal.avl_dict)
    for date_, day_actual in journal.avl_dict.items():
        logger.info('Running process_take1 on %s.', date_)
        day_schedule = journal.schedule[date_]

        for bus, bus_data in day_actual.items():
            # Is this going to be a problem when changing trips?
            current_segment_progress = 0.0
            for time_, report in bus_data.items():
                try:
                    block_number = report['block_number']
                    trip_numbers = report['trip_number']
                    if not trip_numbers:
                        raise ValueError('No trip numbers, not possible!')
                    # FIXME: Handle multiple trips (check last index when adding trip numbers, etc)
                    trip_number = trip_numbers[0]
                    scheduled_stops = day_schedule[block_number][trip_number]['stops']
                    if report['stop_id'] == 0:
                        # FIXME: Does this case capture last-stops? Probably not.
                        # When stop is departing/past a stop
                        stop2stops, merged_line, trip_line_strings = get_shape_trip(scheduled_stops, journal.shapes)
                        progress_distance = get_distance_on_segment_from_report(report, merged_line, current_segment_progress)
                        if progress_distance is None:
                            logger.warning('Skipping [%s].\nRan into issues when getting distance along shape.', report)
                            continue
                        completed_seg, current_segment_progress, distance_from_path = progress_distance
                        day_schedule[bus]['distance_travelled'] = get_segment_length(completed_seg)
                        current_segment = get_trip_progress(stop2stops, trip_line_strings, current_segment_progress)
                        stop_id = current_segment[0]
                        scheduled_stop = scheduled_stops[stop_id]
                        if scheduled_stop['seen'] == 0:
                            scheduled_stop['seen'] += 1
                            segment_shape = journal.shapes[current_segment]
                            scheduled_stop['confidence_factors'].append(int(segment_shape.length//distance_from_path))
                        replace_if_default(scheduled_stop, 'trigger_time', time_)
                        replace_if_default(scheduled_stop, 'operator', report['operator'])
                        scheduled_stop['boards'] += report['boards']
                        scheduled_stop['alights'] += report['alights']
                        scheduled_stop['onboard'] = max(report['onboard'], scheduled_stop['onboard'])
                        scheduled_stop['bus'] = bus

                    # We saw the stop, and know we got there via Avail
                    elif report['stop_id'] in scheduled_stops:
                        stop_id = report['stop_id']
                        scheduled_stop = scheduled_stops[stop_id]
                        scheduled_stop['seen'] += 1
                        scheduled_stop['confidence_factors'].append(100)
                        scheduled_stop['trigger_time'] = time_
                        scheduled_stop['operator'] = report['operator']
                        scheduled_stop['boards'] += report['boards']
                        scheduled_stop['alights'] += report['alights']
                        scheduled_stop['onboard'] = max(report['onboard'], scheduled_stop['onboard'])
                        scheduled_stop['bus'] = bus

                        # TODO: Check to see if going backwards
                        # day_schedule[report['block_number']][report['trip_number']]['seq_tracker'] =
                    else:
                        logger.warning('Stop not in schedule, what happened?\nStop_ID: %s\nBlock: %s\nTrip: %s\n'
                                       'Day: %s', report['stop_id'], report['block_number'], report['trip_number'],
                                       date_)
                        # logger.debug('Cleaning up autogenerated data for this stop.')
                        # del day_schedule[block_number][trip_number]

                except KeyError as e:
                    logger.error('Key does not exist in scheduled_stops. These are the keys:\n'
                                 'block_number=%s\ntrip_number=%s\nKeys in report: %s\nError:\n%s',
                                 report['block_number'], report['trip_number'], report.keys(), e)
                    logger.debug('day_schedule: %s', day_schedule)
        logger.info('Distance traveled on bus %s: %s', bus, day_schedule[bus]['distance_traveled'])


def calculate_confidence(journal: 'Journal'):
    """
    Updates internal book-keeping that could not be done on first sweep. This includes updating confidence values.
    """
    # Calculate confidence scores
    for date_, day_schedule in journal.schedule.items():
        logger.info('Calculating confidence for %s.', date_)
        for block_number, block in day_schedule.items():
            for trip_number, trip in block.items():
                stops = trip['stops']
                for stop_id, stop in stops.items():
                    if stop['seen'] != 0:
                        stop['confidence_score'] = sum(stop['confidence_factors']) / stop['seen']
                    else:
                        stop['confidence_score'] = 0


MAIN_PRESET = {
    'prep': [],
    'main': [process_take1],
    'post': [calculate_confidence]
}

DEFAULT_PROCESSOR_TYPES = MAIN_PRESET.keys()


def get_deflt_processors():
    return {k: [] for k in DEFAULT_PROCESSOR_TYPES}
