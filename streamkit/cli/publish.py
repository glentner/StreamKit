# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Publish messages."""

# standard libs
from sys import stdin
from io import TextIOWrapper
from functools import partial
from argparse import FileType

# internal libs
from ..core.config import ConfigurationError
from ..core.exceptions import log_exception
from ..core.logging import Logger
from ..publisher import Publisher, DEFAULT_TIMEOUT, DEFAULT_BATCHSIZE

# external libs
from cmdkit.app import Application, exit_status
from cmdkit.cli import Interface


# program name is constructed from module file name
PROGRAM = f'streamkit publish'
USAGE = f"""\
usage: {PROGRAM} [-h] NAME TOPIC LEVEL [FILE] [-b SIZE] [-t SEC]
{__doc__}\
"""

HELP = f"""\
{USAGE}

arguments:
NAME                      Name of publisher.
TOPIC                     Name of topic.
LEVEL                     Name of level.
FILE                      Path to file (default <stdin>).

options:
-b, --batch-size    SIZE  Number of messages in a batch.
-t, --timeout       SEC   Timeout in seconds if no messages.
-h, --help                Show this message and exit.\
"""


# initialize module level logger
log = Logger(__name__)


class PublisherApp(Application):
    """Application class for publisher."""

    interface = Interface(PROGRAM, USAGE, HELP)

    topic: str = None
    interface.add_argument('topic')

    level: str = None
    interface.add_argument('level')

    source: TextIOWrapper = stdin
    interface.add_argument('source', nargs='?', type=FileType(mode='r'), default=source)

    batch_size: int = DEFAULT_BATCHSIZE
    interface.add_argument('-b', '--batch-size', type=int, default=batch_size)

    timeout: float = DEFAULT_TIMEOUT
    interface.add_argument('-t', '--timeout', type=float, default=timeout)

    exceptions = {
        RuntimeError: partial(log_exception, log=log.critical,
                              status=exit_status.runtime_error),
        ConfigurationError: partial(log_exception, log=log.critical,
                                    status=exit_status.bad_config),
    }

    def run(self) -> None:
        """Send source lines of text to publisher."""
        with Publisher(topic=self.topic, level=self.level,
                       batchsize=self.batch_size, timeout=self.timeout) as publisher:
            for message in self.source:
                publisher.write(message.strip())
