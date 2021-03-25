import argparse
import logging

format_ = '[%(asctime)s] [%(name)s]: [%(levelname)s] %(message)s'


def read_level_from_args():
    """
    Called when a module wants to get log level using arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log', default='INFO', type=str)
    log_level = parser.parse_args().log
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    return numeric_level


default_log_level = read_level_from_args()


def get_default_logger(name, format__=format_, filepath='./output.log'):
    logger = logging.getLogger(name)
    logger.setLevel(default_log_level)

    ch = logging.StreamHandler()
    ch.setLevel(default_log_level)

    fh = logging.FileHandler(filepath)
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter(format__)
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger
