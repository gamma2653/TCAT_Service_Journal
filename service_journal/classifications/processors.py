from detour_analyzer.trip_analyzer.data_processing import expand_shape_dict
from detour_analyzer.trip_analyzer.segments import track_intervals

from service_journal.gen_utils.class_utils import reorganize_map, DATE_BUS_TIME, DATE_BLOCK_TRIP, \
    sep_shapes_distances, get_shape_trip
from service_journal.gen_utils.debug import get_default_logger

logger = get_default_logger(__name__)


def prep_segment_analysis(journal):
    """
    Code to be run before the primary processing of data. This includes running the data through Jonathan's code so that
     those results can be pulled from when compiling the final service journal.
    """
    # TODO: 1. Get shapes 2. Expand them 3. Convert actuals to Date-Block-Trip 4. Call track_intervals
    converted_actuals = reorganize_map[DATE_BUS_TIME][DATE_BLOCK_TRIP](journal.avl_dict)
    shapes, _ = sep_shapes_distances(journal.shapes)
    logger.debug('Checking if shape 10005 to 1353 exists in shapes: %s', (10005, 1353) in shapes)
    expanded_shapes = expand_shape_dict(shapes)
    logger.debug('Checking if shape 10005 to 1353 exists in EXPANDED SHAPES: %s', (10005, 1353) in expanded_shapes)
    # Go through trips and call track_intervals
    journal.intervals_not_visited = tracked_intervals = {}
    for date_key, date_value in journal.schedule.items():
        for block_key, block_value in date_value.items():
            for trip_key, trip_value in block_value.items():
                stops = list(trip_value['stops'].keys())
                _stop_locations = journal.stop_locations
                stop_locations = [(stop, _stop_locations[stop]) for stop in stops]
                if date_key not in tracked_intervals:
                    tracked_intervals[date_key] = {}
                tracked_intervals_d = tracked_intervals[date_key]
                if block_key not in tracked_intervals_d:
                    tracked_intervals_d[block_key] = {}
                tracked_intervals_db = tracked_intervals_d[block_key]
                trip_shapes = get_shape_trip(stops, expanded_shapes)
                if not converted_actuals[date_key][block_key][trip_key]:
                    logger.warning('converted_actuals for [%s][%s][%s] are empty!', date_key, block_key, trip_key)
                if not trip_shapes:
                    logger.warning('trip_shapes for [%s][%s][%s] are empty! Stops: %s', date_key, block_key, trip_key,
                                   stops)
                tracked_intervals_db[trip_key] = track_intervals(trip_shapes, stop_locations,
                                                                 [(ping['lat'], ping['lon']) for ping in
                                                                  converted_actuals[date_key][block_key][trip_key]])


def process_take1(journal):
    """
    Freshly processed the data in self.schedule and self.avl_dict and updates the schedule's internal book-keeping
    values.
    """
    logger.debug('Processing.\nSchedule: %s\nActuals: %s', journal.schedule, journal.avl_dict)
    for date_, day_actual in journal.avl_dict.items():
        logger.debug('Getting info for: %s', date_)
        day_schedule = journal.schedule[date_]

        for bus, bus_data in day_actual.items():
            for time_, report in bus_data.items():
                try:
                    scheduled_stops = day_schedule[report['block_number']][report['trip_number']]['stops']
                    if report['stop_id'] == 0:
                        pass
                        # Time to infer what happened! Magic time.
                        # trip, lat, lon = report['trip_number'], report['lat'], report['lon']
                        # trip = report['trip_number']

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
                        original_onboard = scheduled_stop['onboard']
                        scheduled_stop['onboard'] = max(report['onboard'], original_onboard)
                        scheduled_stop['bus'] = bus

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


def calculate_confidence(journal):
    """
    Updates internal book-keeping that could not be done on first sweep. This includes updating confidence values.
    """
    # Calculate confidence scores
    for date_, day_schedule in journal.schedule.items():
        for block_number, block in day_schedule.items():
            for trip_number, trip in block.items():
                stops = trip['stops']
                for stop_id, stop in stops.items():
                    if stop['seen'] != 0:
                        stop['confidence_score'] = sum(stop['confidence_factors']) / stop['seen']
                    else:
                        stop['confidence_score'] = 0


def cross_ref_tracked_intervals(journal):
    for date_, day_schedule in journal.schedule.items():
        for block_number, block in day_schedule.items():
            for trip_number, trip in block.items():
                stops = trip['stops']
                for stop_id, stop in stops.items():
                    tracked_was_visited = stop_id not in journal.intervals_not_visited
                    processed_was_visited = stop['seen'] > 0
                    if tracked_was_visited and not processed_was_visited:
                        stop['seen'] += 1
                        stop['confidence_factors'].append(50)


# TODO: make Jonathan code just run in post.
MAIN_PRESET = {
    'prep': [prep_segment_analysis],
    'main': [process_take1],
    'post': [cross_ref_tracked_intervals, calculate_confidence]
}
