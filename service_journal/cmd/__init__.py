# import sys
from typing import Tuple
import argparse
from datetime import date

from gamlogger import get_default_logger

# Dynamic local imports
try:
    from ..classifications.processors import MAIN_PRESET, DEFAULT_PROCESSOR_TYPES
    from ..classifications.journal import Journal
    from ..utilities.utils import interpret_date
except ImportError:
    from classifications.processors import MAIN_PRESET, DEFAULT_PROCESSOR_TYPES
    from classifications.journal import Journal
    from utilities.utils import interpret_date

logger = get_default_logger(__name__)


def input_int(prompt=''):
    try:
        return int(input(prompt))
    except ValueError:
        return input_int(prompt)


def input_(use_argparse: bool = False) -> Tuple[date, date, int]:
    """
    Gets the start and end dates either via argparse or command line. Unless argparse is True, it will prompt the user
    to input a start and end date. It will then return a tuple of these dates interpreted using interpret_date.

    PARAMETERS
    --------
    use_argparse
        If this is true, it will attempt to not have to prompt the user for the dates and use argparse to pull the
        arguments. These would be with the flags -sd (or --start_day) and -ed (or --end_day). Uses interpret_date on
        the acquired string. If it fails to find either of the arguments, it will fall back on default functionality.

    RETURNS
    --------
    Tuple[date, date]
        A tuple containing the start and end dates as date objects.
    """
    if use_argparse:
        parser = argparse.ArgumentParser()
        parser.add_argument('-sd', '--start_day', default=None, type=interpret_date)
        parser.add_argument('-ed', '--end_day', default=None, type=interpret_date)
        parser.add_argument('-b', '--block', default=None, type=int)
        args, _ = parser.parse_known_args()
        if args.start_day is None or args.end_day is None:
            logger.warning('use_argparse is %s, but the requested arguments cannot be found. Switching to manual.',
                           use_argparse)
        else:
            return args.start_day, args.end_day, args.block
    start_date_str = input('Please input the start date in a typical format.\n')
    end_date_str = input('Please input the end date in a typical format.\n')
    block = input('Please enter the block to process, or press enter to process the entire day.\n')
    block = int(block) if block else None
    return interpret_date(start_date_str), interpret_date(end_date_str), block


def run_days(config: bool = None, hold_data: bool = False, use_argparse: bool = False, preset=MAIN_PRESET,
             types_=DEFAULT_PROCESSOR_TYPES):
    """
    Acquires the date range via input_days, then proceeds to read, process, post_process (if post_process is True), and
    write the results to the output connection in journal.

    PARAMETERS
    --------
    config
        The config to pass into Journal when constructing it.
    hold_data
        If True, it will hold onto all the data and write all the changes after all days have been processed. Otherwise,
        it will write changes and clear the journal between days.
    use_argparse
        If True, it will attempt to use command line arguments for the start and end date. Otherwise, it will only rely
        on prompting the user for start and end dates.
    preset
        Preset to install
    types_
        Processors to use
    """
    from_date, to_date, block = input_(use_argparse=use_argparse)
    with Journal(config) as journal:
        journal.install_processor_preset(preset)
        journal.process_dates_batch(from_date, to_date, hold_data, types_, block=block)


def run(use_argparse: bool = False):
    """
    Command line utility to allow continuous prompting of days to run.

    PARAMETERS
    --------
    use_argparse
        If True, it will attempt to use command line arguments for the start and end date. Otherwise, it will only rely
        on prompting the user for start and end dates.
    """
    while True:
        run_days(use_argparse=use_argparse)
        while True:
            user_in = input('Would you like to continue? (Y/n)\n').strip().lower()
            if user_in == 'y':
                break
            elif user_in == 'n':
                return
            else:
                print('Invalid input.')
                continue
