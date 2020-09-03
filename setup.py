# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Build and installation script for StreamKit."""

# standard libs
import os
from setuptools import setup, find_packages

# internal libs
from streamkit.__meta__ import (__pkgname__,
                                __version__,
                                __authors__,
                                __contact__,
                                __license__,
                                __description__)


with open('README.rst', mode='r') as readme:
    long_description = readme.read()


# core dependencies
DEPENDENCIES = ['logalpha>=2.0.2', 'cmdkit>=1.5.4', 'toml>=0.10.1',
                'sqlalchemy>=1.3.19', 'flask>=1.1.2', 'gunicorn>=20.0.4']

# add dependencies for readthedocs.io
if os.environ.get('READTHEDOCS') == 'True':
    DEPENDENCIES.extend(['pydata-sphinx-theme'])


setup(
    name             = __pkgname__,
    version          = __version__,
    author           = __authors__,
    author_email     = __contact__,
    description      = __description__,
    license          = __license__,
    keywords         = 'pub-sub message broker',
    url              = 'https://streamkit.readthedocs.io',
    packages         = find_packages(),
    long_description = long_description,
    long_description_content_type = 'text/x-rst',
    classifiers      = ['Development Status :: 1 - Planning',
                        'Programming Language :: Python :: 3',
                        'Programming Language :: Python :: 3.8',
                        'Programming Language :: Python :: 3.9',
                        'License :: OSI Approved :: Apache Software License', ],
    entry_points     = {'console_scripts': []},
    install_requires = DEPENDENCIES,
    extras_require  = {
        'postgres': ['psycopg2>=2.8.5', ],
    },
)
