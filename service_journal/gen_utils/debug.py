import argparse
import logging

format_ = '[%(asctime)s] [%(name)s]: [%(levelname)s] %(message)s'
date_format = '%m-%d-%Y %H:%M:%S'


def read_level_from_args():
    """
    Called when a module wants to get log level using arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log', default='INFO', type=str)
    args, _ = parser.parse_known_args()
    log_level = args.log
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    return numeric_level


default_log_level = read_level_from_args()
print(f'Default log level set to: {default_log_level}')


def get_default_logger(name, format__=format_, filepath='./output.log', truncate_name: bool = True):
    logger = logging.getLogger(name.split('.')[-1] if truncate_name else name)
    logger.setLevel(default_log_level)

    ch = logging.StreamHandler()
    ch.setLevel(default_log_level)

    fh = logging.FileHandler(filepath)
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter(format__, datefmt=date_format)
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger
