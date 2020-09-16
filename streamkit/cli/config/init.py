# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Initialize default configuration file."""

# standard libs
import os
import functools

# internal libs
from ...core.config import ConfigurationError, init_config, CONF_PATH
from ...core.logging import Logger
from ...core.exceptions import log_exception

# external libs
from cmdkit.app import Application, exit_status
from cmdkit.cli import Interface


PROGRAM = f'streamkit config init'
USAGE = f"""\
usage: {PROGRAM} [-h] {{--user | --system}}
{__doc__}\
"""

HELP = f"""\
{USAGE}

options:
    --system           Initialize system configuration.
    --user             Initialize user configuration.
-h, --help             Show this message and exit.\
"""


# initialize module level logger
log = Logger(__name__)


class InitConfigApp(Application):
    """Application class for config initialization/."""

    interface = Interface(PROGRAM, USAGE, HELP)

    user: bool = False
    system: bool = False
    site_interface = interface.add_mutually_exclusive_group()
    site_interface.add_argument('--user', action='store_true')
    site_interface.add_argument('--system', action='store_true')

    exceptions = {
        RuntimeError: functools.partial(log_exception, log=log.critical,
                                        status=exit_status.runtime_error),
        PermissionError: functools.partial(log_exception, log=log.critical,
                                           status=exit_status.runtime_error),
        ConfigurationError: functools.partial(log_exception, log=log.critical,
                                              status=exit_status.bad_config),
    }

    def run(self) -> None:
        """Business logic of command."""
        self.check_exists()
        init_config(self.cfg_site)

    @property
    def cfg_site(self) -> str:
        """Either 'system' or 'user'."""
        return 'system' if self.system else 'user'

    @property
    def cfg_path(self) -> str:
        """The path to the relevant configuration file."""
        return CONF_PATH[self.cfg_site]

    def check_exists(self) -> None:
        """Check to see if the configuration file already exists."""
        if os.path.exists(self.cfg_path):
            raise RuntimeError(f'{self.cfg_path} exists')
