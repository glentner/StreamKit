# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Subscribe to messages."""

# type annotations
from typing import List

# standard libs
from functools import partial
from queue import Empty

# internal libs
from ..core.config import ConfigurationError
from ..core.exceptions import log_exception
from ..core.logging import Logger
from ..subscriber import Subscriber, DEFAULT_TIMEOUT, DEFAULT_BATCHSIZE, DEFAULT_POLL

# external libs
from cmdkit.app import Application, exit_status
from cmdkit.cli import Interface


# program name is constructed from module file name
PROGRAM = f'streamkit subscribe'
USAGE = f"""\
usage: {PROGRAM} [-h] NAME [TOPIC [TOPIC...]] [-b SIZE] [-p SEC] [-t SEC]
{__doc__}\
"""

HELP = f"""\
{USAGE}

arguments:
NAME                      Name for this subscriber.
TOPIC...                  Names of topics.

options:
-b, --batch-size    SIZE  Number of messages in a batch.
-p, --poll-interval SEC   Seconds to sleep between queries.
-t, --timeout       SEC   Timeout in seconds if no messages.
-h, --help                Show this message and exit.\
"""


# initialize module level logger
log = Logger(__name__)


class SubscriberApp(Application):
    """Application class for subscriber."""

    interface = Interface(PROGRAM, USAGE, HELP)

    name: str = None
    interface.add_argument('name')

    topics: List[str] = []
    interface.add_argument('topics', nargs='+')

    batch_size: int = DEFAULT_BATCHSIZE
    interface.add_argument('-b', '--batch-size', type=int, default=batch_size)

    poll_interval: float = DEFAULT_POLL
    interface.add_argument('-p', '--poll-interval', type=float, default=poll_interval)

    timeout: float = DEFAULT_TIMEOUT
    interface.add_argument('-t', '--timeout', type=float, default=timeout)

    exceptions = {
        RuntimeError: partial(log_exception, log=log.critical,
                              status=exit_status.runtime_error),
        ConfigurationError: partial(log_exception, log=log.critical,
                                    status=exit_status.bad_config),
    }

    def run(self) -> None:
        """Print messages as they arrive."""
        with Subscriber(self.name, self.topics, batchsize=self.batch_size,
                        poll=self.poll_interval, timeout=self.timeout) as stream:
            for message in stream:
                timestamp = message.time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                print(f'{timestamp} {message.host} {message.topic} {message.level} {message.text}')
