# import sys
from typing import Tuple, Iterable
import argparse

from service_journal.gen_utils.debug import get_default_logger
from service_journal.classifications.dbt_classes import Journal
from datetime import date, timedelta, datetime

logger = get_default_logger(__name__)


def date_range(start_date: date, end_date: date) -> Iterable[date]:
    # TODO: Ask if want end_date to be inclusive, just swap for loop for this:
    # for n in range(int((end_date - start_date).days)+1):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def interpret_date(str_: str, formats_: Iterable[str] = ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y')):
    result = None
    for format_ in formats_:
        try:
            result = datetime.strptime(str_, format_)
        except ValueError:
            continue
    if result is None:
        logger.error('No format matches for %s. Formats: %s', str_, formats_)
        raise ValueError('No provided formats matched for provided value. See logs.')
    return result.date()


def input_days(use_argparse: bool = False) -> Tuple[date, date]:
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


def run_days(config: bool = None, hold_data: bool = False, use_argparse: bool = False):
    from_date, to_date = input_days(use_argparse=use_argparse)
    with Journal(config) as journal:
        if hold_data:
            journal.read_days(date_range=date_range(from_date, to_date))
            journal.process()
            journal.post_process()
            journal.write()
        else:
            for day in date_range(from_date, to_date):
                journal.clear()
                journal.read_day(day)
                journal.process()
                journal.post_process()
                journal.write()


def run(use_argparse: bool = False):
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
