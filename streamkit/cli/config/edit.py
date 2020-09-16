# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Edit configuration file."""

# type annotations
from __future__ import annotations

# standard libs
import os
import functools
import subprocess

# internal libs
from ...core.config import init_config, SITE, CONF_PATH, ConfigurationError
from ...core.exceptions import log_exception
from ...core.logging import Logger

# external libs
from cmdkit.app import Application, exit_status
from cmdkit.cli import Interface


# program name is constructed from module file name
PROGRAM = f'streamkit config edit'
USAGE = f"""\
usage: {PROGRAM} [-h] [--system | --user | --local]
{__doc__}\
"""

HELP = f"""\
{USAGE}

The EDITOR environment variable must be set.

options:
    --system         Edit system configuration.
    --user           Edit user configuration.
    --site           Edit local configuration.
-h, --help           Show this message and exit.\
"""


# initialize module level logger
log = Logger(__name__)


class EditConfigApp(Application):
    """Application class for config edit command."""

    interface = Interface(PROGRAM, USAGE, HELP)

    local: bool = False
    user: bool = False
    system: bool = False
    site_interface = interface.add_mutually_exclusive_group()
    site_interface.add_argument('--local', action='store_true')
    site_interface.add_argument('--user', action='store_true')
    site_interface.add_argument('--system', action='store_true')

    exceptions = {
        RuntimeError: functools.partial(log_exception, log=log.critical,
                                        status=exit_status.runtime_error),
        ConfigurationError: functools.partial(log_exception, log=log.critical,
                                              status=exit_status.bad_config),
    }

    def run(self) -> None:
        """Open editor for configuration."""

        site = SITE
        config_path = None
        for key in ('local', 'user', 'system'):
            if getattr(self, key) is True:
                config_path = CONF_PATH[SITE]
                site = key

        if not os.path.exists(config_path):
            log.info(f'{config_path} does not exist - initializing')
            init_config(site)

        if 'EDITOR' not in os.environ:
            raise RuntimeError('EDITOR must be set')

        editor = os.environ['EDITOR']
        subprocess.run([editor, config_path])
