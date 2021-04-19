from typing import Mapping, Iterable, Any


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


def unpack(ordering_: Iterable[Any], data_map_: Mapping[Any]) -> Iterable[Any]:
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


# Default ordering for the output data.
write_ordering = ['date', 'bus', 'report_time', 'dir', 'route', 'block_number', 'trip_number', 'operator', 'boards',
                  'alights', 'onboard', 'stop', 'stop_name', 'sched_time', 'seen', 'confidence_score']
