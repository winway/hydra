import os
import sys
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

if sys.version_info < (2, 6):
    raise EnvironmentError("Python version is too low, need Python>=2.6")

setup(
    name = "mongo_copier",
    version = "1.0",
    author = "wangwei207209",
    author_email = "wangwei207209@sogou-inc.com",
    description = ("Copy MongoDB collections from one mongod to another."),
    license = "BSD",
    keywords = "mongodb copy",
    url = "",
    long_description = read('README.rst'),
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    packages = find_packages(),
    include_package_data = True,
    zip_safe = False,
    install_requires = [
        "pymongo",
        "gevent==1.1;python_version<'2.7'",
        "gevent>=1.2;python_version>='2.7'",
    ],
    entry_points = {
        "console_scripts": [
            "mongo_copier = mongo_copier.copy_collection:main",
        ],
    },
)
