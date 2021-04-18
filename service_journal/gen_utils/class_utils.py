def pull_out_name(d):
    return {k: v['name'] for k, v in d.items()}


def unpack(ordering_, data_map_):
    return [data_map_[i] for i in ordering_]


write_ordering = ['date', 'bus', 'report_time', 'dir', 'route', 'block_number', 'trip_number', 'operator', 'boards',
                  'alights', 'onboard', 'stop', 'stop_name', 'sched_time', 'seen', 'confidence_score']
