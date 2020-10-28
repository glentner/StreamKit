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
import logging

# internal libs
from ..__meta__ import __version__, __website__
from ..core.logging import initialize_logging

# external libs
from cmdkit.app import Application, ApplicationGroup
from cmdkit.cli import Interface

# command groups
from . import config, subscribe, publish, database


USAGE = f"""\
usage: streamkit [-h] [-v] <command> [<args>...]
Command-line tools for StreamKit.\
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
log = logging.getLogger(__name__)

# basic configuration will write to stderr,
# formatting and level from config file
initialize_logging()


# inject logger back into cmdkit library
Application.log_critical = log.critical
Application.log_exception = log.exception


class StreamKit(ApplicationGroup):
    """Application class for streamkit entry-point."""

    interface = Interface('streamkit', USAGE, HELP)
    interface.add_argument('-v', '--version', version=__version__, action='version')
    interface.add_argument('command')

    command = None
    commands = {'config': config.ConfigApp,
                'publish': publish.PublisherApp,
                'subscribe': subscribe.SubscriberApp,
                'database': database.DatabaseApp}


def main() -> int:
    """Entry-point for streamkit command-line interface."""
    return StreamKit.main(sys.argv[1:])
