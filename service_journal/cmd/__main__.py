try:
    from . import run
except ImportError:
    from cmd import run

run(use_argparse=True)
