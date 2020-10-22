# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Database configuration for StreamKit."""


# type annotations
from __future__ import annotations

# standard libs
import logging
from urllib.parse import urlencode

# external libs
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError

# internal libs
from ...core.config import config, ConfigurationError


# initialize module level logger
log = logging.getLogger(__name__)


def get_url(**params) -> str:
    """
    Construct complete URL from configuration file.

    Something like:
        backend://[[user[:password]@]host[:port]]/database[?params...]

    Parameters:
        **params:
            Named parameters to construct the URL. Reserved names are popped
            from the dictionary, remaining parameters are url-encoded at the end.
            Reserved names include "backend", "user", "password", "host", "port",
            and "database".

    Returns:
        _url (str):
            The constructed URL string.
    """
    _url = ''
    try:
        backend = params.pop('backend')
        _url += f'{backend}://'

        user = params.pop('user', None)
        password = params.pop('password', None)
        if user and password:
            _url += f'{user}:{password}@'
        elif user and not password:
            _url += f'{user}@'
        elif password and not user:
            raise ConfigurationError('get_url: `password` given but not `user`')

        host = params.pop('host', None)
        port = params.pop('port', None)
        if host and port:
            _url += f'{host}:{port}'
        elif host and not port:
            _url += f'{host}'
        elif port and not host:
            _url += f'localhost:{port}'

        database = params.pop('database', None)
        if database:
            _url += f'/{database}'

        if params:
            encoded_params = urlencode(params)
            _url += f'?{encoded_params}'

        return _url

    except KeyError as _error:
        raise ConfigurationError(*_error.args) from _error


db_config = config['database'].copy()

# NOTE: For TimescaleDB we actually are PostgreSQL
overrides = dict()
if db_config['backend'] in ('timescale', 'timescaledb'):
    overrides['backend'] = 'postgres'

schema = db_config.pop('schema', None)
connect_args = db_config.pop('connect_args', {})
url = get_url(**{**db_config, **overrides})

try:
    engine = create_engine(url, connect_args=connect_args)
except ArgumentError as error:
    log.critical(f'bad URL: {url}')
    log.critical('check your configuration and environment variables')
    raise ConfigurationError(url) from error
