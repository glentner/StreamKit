# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Initialize database."""


# standard libs
import functools
import logging

# internal libs
from ...core.config import ConfigurationError
from ...core.exceptions import log_exception
from ...database.core.init import init, init_test_data, init_extensions
from ...database.core.engine import db_config

# external libs
from cmdkit.app import Application, exit_status
from cmdkit.cli import Interface
from sqlalchemy.exc import DatabaseError


PROGRAM = 'streamkit database init'
USAGE = f"""\
usage: {PROGRAM} [-h] [--test] [--ext]
{__doc__}\
"""

HELP = f"""\
{USAGE}

options:
    --echo             Dump SQL as emitted. 
    --test             Insert test dataset.
    --ext              Initialize with extensions (if applicable).
-h, --help             Show this message and exit.\
"""


# initialize module level logger
log = logging.getLogger(__name__)


class InitDatabaseApp(Application):
    """Application class for database init entry-point."""

    interface = Interface(PROGRAM, USAGE, HELP)
    ALLOW_NOARGS = True

    include_test_data: bool = False
    interface.add_argument('--test', action='store_true', dest='include_test_data')

    include_extensions: bool = False
    interface.add_argument('--ext', action='store_true', dest='include_extensions')

    echo: bool = False
    interface.add_argument('--echo', action='store_true')

    exceptions = {
        RuntimeError: functools.partial(log_exception, log=log.critical,
                                        status=exit_status.runtime_error),
        DatabaseError: functools.partial(log_exception, log=log.critical,
                                         status=exit_status.runtime_error),
        ConfigurationError: functools.partial(log_exception, log=log.critical,
                                              status=exit_status.bad_config),
    }

    def run(self) -> None:
        """Business logic of command."""
        self.check_backend()
        self.config_engine()
        init()
        if self.include_extensions:
            init_extensions()
        if self.include_test_data:
            init_test_data()

    @staticmethod
    def check_backend() -> None:
        """Check if we are touching an external database."""
        backend = db_config['backend']
        if backend != 'sqlite':
            response = input(f'Connecting to {backend} database, proceed [Y/n]: ')
            if response in ('Yes', 'yes', 'Y', 'y'):
                pass
            elif response in ('No', 'no', 'N', 'n'):
                raise RuntimeError('Stopping now')
            else:
                raise RuntimeError('Response not understood')

    def config_engine(self) -> None:
        """Set `echo` parameter for engine."""
        if self.echo:
            from ...database.core.engine import engine
            engine.echo = True
            for name, value in db_config.items():
                log.debug(f'{name}: {value}')
