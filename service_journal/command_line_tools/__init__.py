# import sys
from typing import Tuple, Iterable
import argparse

from service_journal.gen_utils.debug import get_default_logger
from service_journal.classifications.dbt_classes import Journal
from datetime import date, timedelta, datetime

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


def input_days(use_argparse: bool = False) -> Tuple[date, date]:
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
        args = parser.parse_args()
        if args.start_day is not None and args.end_day is not None:
            return args.start_day, args.end_day
        else:
            logger.warning('use_argparse is %s, but the requested arguments cannot be found. Switching to manual.',
                           use_argparse)
    start_date_str = input('Please input the start date in a typical format.\n')
    end_date_str = input('Please input the end date in a typical format.\n')
    return interpret_date(start_date_str), interpret_date(end_date_str)


def run_days(config: bool = None, hold_data: bool = False, use_argparse: bool = False, post_process: bool = True):
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
    post_process
        If True, it will run the post_process method after running the process method.
    """
    from_date, to_date = input_days(use_argparse=use_argparse)
    with Journal(config) as journal:
        journal.process_dates_batch(from_date, to_date, hold_data, post_process)


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
