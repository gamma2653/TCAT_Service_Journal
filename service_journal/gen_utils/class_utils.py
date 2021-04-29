from enum import Enum
from typing import Mapping, Iterable, Any, Callable
from service_journal.gen_utils.debug import get_default_logger


logger = get_default_logger(__name__)


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


class OrganizeOrder(Enum):
    DATE_BLOCK_TRIP = 'DATE_BLOCK_TRIP'
    DATE_BUS_TIME = 'DATE_BUS_TIME'


DATE_BLOCK_TRIP = OrganizeOrder.DATE_BLOCK_TRIP
DATE_BUS_TIME = OrganizeOrder.DATE_BUS_TIME


def _block_2_bus(mapping: Mapping) -> Mapping:
    result = {}
    for date, date_value in mapping.items():
        for block, block_value in date_value.items():
            for trip, trip_value in block_value.items():
                for stop in trip_value:
                    bus = stop['bus']
                    trigger_time = stop['time']
                    if bus is None or trigger_time is None:
                        logger.warning('Bus or trigger time is None, this should not happen.\nBus:%s\nTrigger Time:%s',
                                       bus, trigger_time)
                    if date not in result:
                        result[date] = {}
                    if bus not in result[date]:
                        result[date][bus] = {}
                    result[date][bus][trigger_time] = {
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
    for date, date_value in mapping.items():
        for bus, bus_value in date_value.items():
            for trigger, trigger_value in bus_value.items():
                block = trigger_value['block_number']
                trip = trigger_value['trip_number']
                if date not in result:
                    result[date] = {}
                if block not in result[date]:
                    result[date][block] = {}
                if trigger not in result[date][block]:
                    result[date][block][trigger] = []
                result[date][block][trip].append({
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
