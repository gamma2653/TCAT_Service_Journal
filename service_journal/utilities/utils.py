from datetime import datetime, date, timedelta
from enum import Enum
from collections import defaultdict
from typing import Mapping, Iterable, Any, Callable, Tuple, List, Set, Sequence, MutableMapping

from shapely.geometry import Point, LineString
from shapely.geometry.base import BaseGeometry
from shapely.ops import split as shapely_split, nearest_points, linemerge
import geopy.distance
from geopy.distance import distance as geo_distance


from .debug import get_default_logger


logger = get_default_logger(__name__)


def date_range(start_date: date, end_date: date) -> Iterable[date]:
    """
    Yields a generator of dates starting with start_date and ending just before end_date.

    PARAMETERS
    ---------
    start_date
        The first date to yield
    end_date
        The day after the last day to yield

    RETURNS
    --------
    Iterable[date]
        A generator that yields the days between start and end date.
    """
    # TODO: Ask if want end_date to be inclusive, just swap for loop for this:
    # for n in range(int((end_date - start_date).days)+1):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def def_dict():
    """
    Creates a default dictionary with default dictionaries as values
    """
    return defaultdict(def_dict)


def interpret_date(str_: str, formats_: Iterable[str] = ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y')) -> date:
    """
    Interprets the passed in string as a date object. Cycles through all possible formats and returns first valid one.

    PARAMETERS
    --------
    str_
        String to interpret as a date.
    formats_
        Iterable of strings that are potential formats.

    RETURNS
    --------
    date
        A date object represented by the passed in string.
    """
    result = None
    for format_ in formats_:
        try:
            result = datetime.strptime(str_, format_)
            break
        except ValueError:
            continue
    if result is None:
        logger.error('No format matches for %s. Formats: %s', str_, formats_)
        raise ValueError('No provided formats matched for provided value. See logs.')
    return result.date()


def str_2_coord_tuple(coord_str: str) -> Tuple[float, float]:
    try:
        lat, lon = coord_str.split()
        return float(lat), float(lon)
    except ValueError as exc:
        logger.error('Malformed coordinate: (%s)', coord_str)
        raise ValueError(f'Malformed coordinate: ({coord_str})') from exc


def sep_shapes_distances(shapes_and_lengths: Mapping[Tuple[int, int], Tuple[float, BaseGeometry]]):
    # TODO: Use zip and dict constructor
    shapes, shape_distances = {}, {}
    for key, (distance, shape) in shapes_and_lengths.items():
        shape_distances[key], shapes[key] = distance, shape
    return shapes, shape_distances


def _check_for_dups(path: List[Tuple[float, float]]) -> Set[Tuple[float, float]]:
    """
    Utility function for testing only. Checks to see if there are back-to-back repeated points in a path.
    """
    last_point = None
    dups = set()
    for point in path:
        print(f'Comparing {point} and {last_point}, getting {point==last_point}')
        if point == last_point:
            dups.add(point)
        last_point = point
    return dups


def pull_out_name(d: Mapping[str, Mapping]) -> Mapping[str, str]:
    """
    Turns a mapping of:
    {
        [name] : {
            'name': [sub_name]
            ...
        }
        ...
    }
    into a mapping of this shape:
    {
        [name] : [sub_name]
        ...
    }

    PARAMETERS
    --------
    d
        Mapping where the values are dictionaries with the key "name."

    RETURNS
    --------
    Mapping[str, str]
        Mappings where the inputs' values' value of the 'name' key has become the new value.
    """
    return {k: v['name'] for k, v in d.items()}


def unpack(ordering_: Iterable[str], data_map_: Mapping[str, Any]) -> Iterable[Any]:
    """
    Returns the values of the dictionary in the order of the passed in list.

    PARAMETERS
    --------
    ordering_
        An iterable list of keys from data_map_
    data_map_
        A mapping of unordered values

    RETURNS
    --------
    Iterable[Any]
        An Iterable generator of values that are ordered according to ordering_
    """
    return (data_map_[i] for i in ordering_)


DEFAULT_VALUES = {[], 0, None}


def replace_if_default(map_: MutableMapping, key, value):
    if key in map_ and map_[key] in DEFAULT_VALUES:
        map_[key] = value


class OrganizeOrder(Enum):
    DATE_BLOCK_TRIP = 'DATE_BLOCK_TRIP'
    DATE_BUS_TIME = 'DATE_BUS_TIME'


DATE_BLOCK_TRIP = OrganizeOrder.DATE_BLOCK_TRIP
DATE_BUS_TIME = OrganizeOrder.DATE_BUS_TIME


def _block_2_bus(mapping: Mapping) -> Mapping:
    result = {}
    for date_key, date_value in mapping.items():
        for block, block_value in date_value.items():
            for trip, trip_value in block_value.items():
                for stop in trip_value:
                    bus = stop['bus']
                    trigger_time = stop['time']
                    if bus is None or trigger_time is None:
                        logger.warning('Bus or trigger time is None, this should not happen.\nBus:%s\nTrigger Time:%s',
                                       bus, trigger_time)
                    if date_key not in result:
                        result[date_key] = {}
                    if bus not in result[date_key]:
                        result[date_key][bus] = {}
                    result[date_key][bus][trigger_time] = {
                        'lat': stop.get('lat', None),
                        'lon': stop.get('lon', None),
                        'dir': stop.get('dir', None),
                        'operator': stop.get('operator', None),
                        'depart': stop.get('depart', None),
                        'boards': stop.get('boards', 0),
                        'alights': stop.get('alights', 0),
                        'onboard': stop.get('onboard', 0),
                        'stop_id': stop.get('stop_id', None),
                        'name': stop.get('name', None),
                        'block_number': block,
                        'route': stop.get('route', set()),
                        'trip_number': trip,
                    }
    return result


def _bus_2_block(mapping: Mapping) -> Mapping:
    result = {}
    for date_key, date_value in mapping.items():
        for bus, bus_value in date_value.items():
            for trigger, trigger_value in bus_value.items():
                block = trigger_value['block_number']
                trip = trigger_value['trip_number']
                if date_key not in result:
                    result[date_key] = {}
                if block not in result[date_key]:
                    result[date_key][block] = {}
                if trip not in result[date_key][block]:
                    result[date_key][block][trip] = []
                result[date_key][block][trip].append({
                    'alights': trigger_value.get('alights', 0),
                    'boards': trigger_value.get('boards', 0),
                    'onboard': trigger_value.get('onboard', 0),
                    'bus': bus,
                    'date_time': trigger,
                    'day': trigger_value.get('day', None),
                    'depart': trigger_value.get('depart', None),
                    'dir': trigger_value.get('dir', None),
                    'lat': trigger_value.get('lat', None),
                    'lon': trigger_value.get('lon', None),
                    'operator': trigger_value.get('operator', None),
                    'stop_id': trigger_value.get('stop_id', None),
                    'time': trigger,
                })
    return result


reorganize_map: Mapping[OrganizeOrder, Mapping[OrganizeOrder, Callable[[Mapping], Mapping]]] = {
    OrganizeOrder.DATE_BUS_TIME: {
        OrganizeOrder.DATE_BLOCK_TRIP: _bus_2_block,
        OrganizeOrder.DATE_BUS_TIME: lambda x: x
    },
    OrganizeOrder.DATE_BLOCK_TRIP: {
        OrganizeOrder.DATE_BUS_TIME: _block_2_bus,
        OrganizeOrder.DATE_BLOCK_TRIP: lambda x: x
    }
}


# Default ordering for the output data.
write_ordering = ['date', 'bus', 'report_time', 'dir', 'route', 'block_number', 'trip_number', 'operator', 'boards',
                  'alights', 'onboard', 'stop', 'stop_name', 'sched_time', 'seen', 'confidence_score']


def get_shape_trip(stops: Sequence[int], shapes: Mapping[Tuple[int, int], BaseGeometry]) -> \
        Tuple[Sequence[Tuple[int, int]], LineString, Sequence[BaseGeometry]]:
    """
    Gives shapes along stops on a given trip.

    PARAMETERS
    --------
    stops
        Contains stops on a given trip and is ordered.
    shapes
        Mapping of from stop to stop that gives an iterable of coordinates that define a shape.
    """
    logger.info('get_shape_trip on stops: %s', stops)
    trip_line_strings = []
    stop2stops = []
    for i in range(len(stops)-1):
        try:
            stop2stop_key = stops[i], stops[i+1]
            stop2stops.append(stop2stop_key)
            trip_line_strings.append(shapes[stop2stop_key])
        except IndexError:
            continue
    merged_line = linemerge(trip_line_strings)
    # Check orientation of merged line, and if backwards flip
    if merged_line.coords[0] != trip_line_strings[0].coords[0]:
        merged_line = LineString(merged_line.coords[::-1])
    return stop2stops, merged_line, trip_line_strings


def get_current_distance_on_trip(point: Point, line: LineString, current_shape_progress: float) -> \
        Tuple[float, geopy.distance.distance]:
    _, remaining_line = shapely_split(line, line.interpolate(current_shape_progress))
    _, point_on_line = nearest_points(point, remaining_line)
    completed, _ = shapely_split(line, point_on_line)
    return completed.length, geo_distance(point.coords, point_on_line.coords)


def get_segment_length(line: LineString) -> geopy.distance.distance:
    points = line.coords
    sum_distance = geo_distance((0, 0), (0, 0))
    if len(points) < 2:
        return sum_distance
    for i in range(len(points)-1):
        sum_distance += geo_distance(points[i].coords, points[i+1].coords)
    return sum_distance


def get_trip_progress(stop2stops: Sequence[Tuple[int, int]], trip_line_strings, progress):
    for i, segment in enumerate(trip_line_strings):
        if progress-segment.length < 0:
            return stop2stops[i]
        progress -= segment.length
    logger.warning('get_progress got a distance that was longer than all of the shapes put together.')


def get_distance_on_segment_from_report(report: Mapping[str, Any], trip_shape: LineString,
                                        prev_shape_progress: float = 0.0) -> Tuple[float, geopy.distance.distance]:
    """
    Returns where we currently are on the LineString of the trip, based on the prev_index.

    PARAMETERS
    --------
    report
        stop report
    trip_shape
        path to path
    prev_shape_progress
        last index that was observed
    """
    return get_current_distance_on_trip(Point(report['lon'], report['lat']), trip_shape, prev_shape_progress)
