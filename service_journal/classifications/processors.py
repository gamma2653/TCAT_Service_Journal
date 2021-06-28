from typing import TYPE_CHECKING
from service_journal.gen_utils.debug import get_default_logger

if TYPE_CHECKING:
    from service_journal.classifications.dbt_classes import Journal

logger = get_default_logger(__name__)


def build_trip_shapes(journal: 'Journal'):
    journal.trip_shapes = {}


def post_segment_analysis(journal: 'Journal'):
    """
    Code to be run before the primary processing of data. This includes running the data through Jonathan's code so that
     those results can be pulled from when compiling the final service journal.
    """
    # TODO: 1. Get shapes 2. Expand them 3. Convert actuals to Date-Block-Trip 4. Call track_intervals
    pass


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
                        scheduled_stop['onboard'] = max(report['onboard'], scheduled_stop['onboard'])
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


def cross_ref_tracked_intervals(journal: 'Journal'):
    return
    # for date_, day_schedule in journal.schedule.items():
    #     logger.info('Cross referencing %s.', date_)
    #     intervals_not_seen_d = journal.intervals_not_visited.get(date_, {})
    #     for block_number, block in day_schedule.items():
    #         intervals_not_seen_db = intervals_not_seen_d[block_number]
    #         for trip_number, trip in block.items():
    #             stops = trip['stops']
    #             intervals_not_seen_dbt = intervals_not_seen_db[trip_number]
    #             for stop_id, stop in stops.items():
    #                 tracked_was_visited = stop_id not in intervals_not_seen_dbt
    #                 processed_was_visited = stop['seen'] > 0
    #                 if tracked_was_visited and not processed_was_visited:
    #                     logger.debug('stop_id: %s\nintervals_not_seen_dbt: %s\ntracked_was_visited: %s\n'
    #                                  'processed_was_visited: %s', stop_id, intervals_not_seen_dbt, tracked_was_visited,
    #                                  processed_was_visited)
    #                     stop['seen'] += 1
    #                     stop['confidence_factors'].append(50)


# TODO: make Jonathan code just run in post.
MAIN_PRESET = {
    'prep': [],
    'main': [process_take1],
    'post': [post_segment_analysis, cross_ref_tracked_intervals, calculate_confidence]
}

DEFAULT_PROCESSOR_TYPES = MAIN_PRESET.keys()


def get_deflt_processors():
    return {k: [] for k in DEFAULT_PROCESSOR_TYPES}
