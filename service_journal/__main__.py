try:
    from .cmd import run
except ImportError:
    from service_journal.cmd import run

run(use_argparse=True)