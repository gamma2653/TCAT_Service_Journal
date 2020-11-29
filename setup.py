from setuptools import setup, find_packages
def readme():
    data = ''
    with open('README.md', 'r') as f:
        data = f.read()
    return data
setup(
    name="service_journal",
    author="Christopher De Jesus",
    author_email="cd525@cornell.edu",
    description="A framework for journaling TCAT bus activity.",
    long_description=readme(),
    long_description_content_type="text/markdown",
    version="2.1",
    packages=find_packages(),
)
