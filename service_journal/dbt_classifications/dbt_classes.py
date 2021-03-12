# DBT_Classes = Day-Block-Trip classes
import sys
from collections import OrderedDict
# from service_journal.dbt_classifications.exceptions import BlockNotFound, TripNotFound, NotActual, NotSchedule
from enum import IntFlag, Enum
from datetime import datetime, timedelta

from service_journal.gen_utils.debug import Logger
from service_journal.sql_handler.connection import Connection, DataFormat

# Logger initialization
logger = Logger(__name__)
logger.read_args()


class SortRules(Enum):
    """
    An enum of sort rules.
    TODO: Replace usage with a sort function that gets passed.
    """
    BY_BUS = 1


class Flag(IntFlag):
    """
    A binary flag system used to flag deviations from the norm in a given stop
    or trip.
    """
    FINE = 0
    BACKWARDS_TIME = 1
    MULTIPLE_BUS_BLOCK = 2


def closest_stop_id(stop_locations, loc):
    """
    Parameters:
    [stop_locations] a dictionary with stop ids, "stop_id", as the key, and geographical
    stop locations, "stop_loc", as the value.
    "stop_loc" should be index-able by 0 and 1. {stop_loc[0]} should be longitude
    and {stop_loc[1]} should be latitude, as per convention.

    [loc] is the location we are trying to find the closest stop id to.
    """
    min_ = sys.maxsize
    closest_stop = 0
    for stop_id, stop_loc in stop_locations.items():
        dist = (float(stop_loc[0]) - float(loc[0])) ** 2 + (float(stop_loc[1]) - float(loc[1])) ** 2
        if dist <= min_:
            closest_stop = stop_id
            min_ = dist
    return closest_stop


class Journal:
    """
    An class used to instantiate a journal. Should be able to accept multiple
    dates and store them all.

    NOTE: Currently bugged. Not able to store multiple dates for unknown reason.
    TODO: Fix this problem, or figure out if I was making a silly mistake when
    adding a new day.
    """

    def __init__(self, schedule: dict = None, avl_dict: dict = None):
        self.schedule = schedule if schedule is not None else {}
        self.avl_dict = avl_dict if avl_dict is not None else {}

    def update(self, schedule=None, avl_dict=None):
        if schedule is not None:
            self.schedule.update(schedule)
        if avl_dict is not None:
            self.avl_dict.update(avl_dict)


    def read_days(self, date_range):
        with Connection() as conn:
            for day in date_range:
                self.schedule[day], self.avl_dict[day] = conn.read(day, format_=conn.DBT)

    def process(self):
        for date_, day in self.avl_dict.items():
            day_schedule = self.schedule[date_]

            for bus, bus_data in day.items():
                for time_, report in bus_data.items():




    def __str__(self, indent=4, tab_char=' '):
        tab = ' ' * indent if tab_char == ' ' else '\t'
        print_out = '[Days]'
        for date, day in self.root.items():
            print_out += f'{date}:\n'
            print_out += f'{tab}[Blocks]\n'
            for blockNumber, block in day.items():
                print_out += f'{tab}{blockNumber}:\n'
                print_out += f'{tab * 2}[Trips]\n'
                for tripNumber, trip in block.items():
                    # Did it this disgusting way because I wanted stops printed last.
                    print_out += f'{tab * 2}{tripNumber}:\n'
                    print_out += f'{tab * 3}route: {trip["route"]}\n'
                    print_out += f'{tab * 3}direction: {trip["direction"]}\n'
                    print_out += f'{tab * 3}actual_start: {trip["actual_start"]}\n'
                    print_out += f'{tab * 3}actual_end: {trip["actual_end"]}\n'
                    print_out += f'{tab * 3}flag: {trip["flag"]}\n'
                    print_out += f'{tab * 3}stops:\n'
                    for (stop_id, stop_inst), stop in trip['stops'].items():
                        print_out += f'{tab * 4}{stop_id} / {stop_inst}\n'
                        for key, value in stop.items():
                            print_out += f'{tab * 5}{key}: {value}\n'
        return print_out
