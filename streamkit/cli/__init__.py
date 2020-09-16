# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Command-line interface for StreamKit."""

# standard libs
import sys

# internal libs
from ..core.logging import Logger
from ..__meta__ import __version__, __website__

# external libs
from cmdkit import logging as _cmdkit_logging
from cmdkit.app import Application, ApplicationGroup
from cmdkit.cli import Interface

# command groups
from . import config, subscribe, publish, database


COMMANDS = {
    'config': config.ConfigApp,
    'publish': publish.PublisherApp,
    'subscribe': subscribe.SubscriberApp,
    'database': database.DatabaseApp,
}

USAGE = f"""\
usage: streamkit [-h] [-v] <command> [<args>...]
Command-line tools for Streamkit.\
"""

EPILOG = f"""\
Documentation and issue tracking at:
{__website__}\
"""

HELP = f"""\
{USAGE}

commands:
publish                {publish.__doc__}
subscribe              {subscribe.__doc__}
database               {database.__doc__}
config                 {config.__doc__}

options:
-h, --help             Show this message and exit.
-v, --version          Show the version and exit.

Use the -h/--help flag with the above commands to
learn more about their usage.

{EPILOG}\
"""


# initialize module level logger
log = Logger('streamkit')


# inject logger back into cmdkit library
_cmdkit_logging.log = log
Application.log_error = log.critical


class StreamKit(ApplicationGroup):
    """Application class for streamkit entry-point."""

    interface = Interface('streamkit', USAGE, HELP)
    interface.add_argument('-v', '--version', version=__version__, action='version')
    interface.add_argument('command')

    command = None
    commands = COMMANDS


def main() -> int:
    """Entry-point for streamkit command-line interface."""
    return StreamKit.main(sys.argv[1:])
