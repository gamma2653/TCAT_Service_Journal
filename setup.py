import os
import json


# FIXME: Currently AUTHORS gets saved as the license automatically by setuptools. This isn't an issue, but can lead to
#   future confusion.


_env_falsey_vals = ['n', 'no', 'false', '0']
name = 'TCAT_Service_Journal'

dirs = {
    'backup_packages': 'PACKAGES',
    'readme': 'README.rst',
    'version': 'service_journal/__init__.py',
    'authors': 'AUTHORS',
}

try:
    if os.environ.get('PYTHON_USE_SETUPTOOLS', '').lower() in _env_falsey_vals:
        raise ImportError
    # noinspection PyUnresolvedReferences
    from setuptools import setup, find_packages
    SETUPTOOLS = True
except ImportError:
    from distutils.core import setup
    SETUPTOOLS = False

description = ''


def _load_authors():
    f = open(dirs['authors'], 'r')
    try:
        author_data = f.read().split('\n')
        return author_data[0], author_data[1], author_data[2]
    except IndexError:
        e = 'Error retrieving Authors (Malformed)'
        return e, e, e
    finally:
        f.close()


def _load_manual_packages():
    with open(dirs['packages'], 'r') as f:
        return json.load(f)['packages']


# Used if SETUPTOOLS is false
_packages = find_packages() if SETUPTOOLS else _load_manual_packages()
print('SETUPTOOLS=', SETUPTOOLS)
print('packages=', _packages)


def readme(_readme):
    with open(os.path.join(os.path.dirname(__file__), _readme), 'r') as f:
        return f.read()


def get_short_description(_readme: str):
    lines = _readme.split('\n')
    idx = -1
    for i in range(len(lines)):
        if lines[i].startswith(name):
            # 2 indexes forward because of rst formatting
            idx = i+2
            break
    if idx != -1:
        return lines[idx]
    return 'No description found.'


def get_version():
    # Inspired by https://stackoverflow.com/questions/458550/standard-way-to-embed-version-into-python-package
    import re
    VERSIONFILE=dirs['version']
    verstrline = open(VERSIONFILE, "rt").read()
    VSRE = r"^__version__ = \((\d+), (\d+), (\d+)\)"
    mo = re.search(VSRE, verstrline, re.M)
    if mo:
        vertup = mo.group(1)
    else:
        raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))
    verstr = ".".join(vertup)

def _setup():
    print('name=', name)
    long_description = readme(dirs['readme'])
    short_description = get_short_description(long_description)
    print('short_description=', short_description)
    author, author_email, url = _load_authors()
    print('author=', author)
    print('author_email=', author_email)
    print('url=', url)
    version = get_version()
    print('version=', version)
    setup(
        name=name,
        version=version,
        author=author,
        author_email=author_email,
        url=url,
        description=short_description,
        long_description=long_description,
        packages=_packages
    )


_setup()
