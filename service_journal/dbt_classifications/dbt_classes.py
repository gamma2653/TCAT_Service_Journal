# DBT_Classes = Day-Block-Trip classes
from datetime import date, datetime, timedelta
from typing import Iterable, Mapping

from service_journal.sql_handler.connection import Connection, DataFormat
from service_journal.gen_utils.debug import get_default_logger
logger = get_default_logger(__name__)


class Journal:
    """
    An class used to instantiate a journal. Should be able to accept multiple
    dates and store them all.

    NOTE: Currently bugged. Not able to store multiple dates for unknown reason.
    TODO: Fix this problem, or figure out if I was making a silly mistake when
    adding a new day.
    """

    def __init__(self, schedule: Mapping = None, avl_dict: Mapping = None):
        self.schedule = {} if schedule is None else schedule
        self.avl_dict = {} if avl_dict is None else avl_dict

    def update(self, schedule: Mapping = None, avl_dict: Mapping = None) -> None:
        """
        Updates the schedule and/or avl_dict with the given parameters.
        """
        if schedule is not None:
            self.schedule.update(schedule)
        if avl_dict is not None:
            self.avl_dict.update(avl_dict)

    def read_days(self, date_range: Iterable[date]) -> None:
        """
        Read date_range from a newly established connection.
        """
        with Connection() as conn:
            for day in date_range:
                self.schedule[day], self.avl_dict[day] = conn.read(day, format_=conn.DBT)

    def process(self) -> None:
        # def approx_schedule_trip(schedule, date, block_number, trip, trigger_time)
        for date_, day_actual in self.avl_dict.items():
            day_schedule = self.schedule[date_]

            for bus, bus_data in day_actual.items():
                for time_, report in bus_data.items():
                    scheduled_stops = day_schedule[report['block_number']][report['trip_number']]['stops']

    # Out of date
    # def __str__(self, indent: int = 4, tab_char: str = ' '):
    #     tab = ' ' * indent if tab_char == ' ' else '\t'
    #     print_out = '[Days]'
    #     for date_, day in self.root.items():
    #         print_out += f'{date_}:\n'
    #         print_out += f'{tab}[Blocks]\n'
    #         for blockNumber, block in day.items():
    #             print_out += f'{tab}{blockNumber}:\n'
    #             print_out += f'{tab * 2}[Trips]\n'
    #             for tripNumber, trip in block.items():
    #                 # Did it this disgusting way because I wanted stops printed last.
    #                 print_out += f'{tab * 2}{tripNumber}:\n'
    #                 print_out += f'{tab * 3}route: {trip["route"]}\n'
    #                 print_out += f'{tab * 3}direction: {trip["direction"]}\n'
    #                 print_out += f'{tab * 3}actual_start: {trip["actual_start"]}\n'
    #                 print_out += f'{tab * 3}actual_end: {trip["actual_end"]}\n'
    #                 print_out += f'{tab * 3}flag: {trip["flag"]}\n'
    #                 print_out += f'{tab * 3}stops:\n'
    #                 for (stop_id, stop_inst), stop in trip['stops'].items():
    #                     print_out += f'{tab * 4}{stop_id} / {stop_inst}\n'
    #                     for key, value in stop.items():
    #                         print_out += f'{tab * 5}{key}: {value}\n'
    #     return print_out
