# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""
Logging configuration for StreamKit.

StreamKit uses the `logalpha` package for logging functionality. All messages
are written to <stderr> and should be redirected by their parent processes.

Levels:
    DEBUG      Low level notices (e.g., database connection).
    INFO       Informational messages of general interest.
    WARNING    Something unexpected or possibly problematic occurred.
    ERROR      An error caused an action to not be completed.
    CRITICAL   The entire application must halt.

Handlers:
    STANDARD   Simple colorized console output. (no metadata)
    DETAILED   Detailed (syslog-style) messages (with metadata)

Environment Variables:
    STREAMKIT_LOGGING_LEVEL      INT or NAME of logging level.
    STREAMKIT_LOGGING_HANDLER    STANDARD or DETAILED
"""

# type annotations
from __future__ import annotations
from typing import List, Callable, Any

# standard libraries
import io
import sys
import socket
from datetime import datetime
from dataclasses import dataclass

# external libraries
from logalpha import levels, colors, messages, handlers, loggers


# get hostname from `socket` instead of `.config`
HOSTNAME = socket.gethostname()


# logging levels associated with integer value and color codes
LEVELS = levels.Level.from_names(('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))
COLORS = colors.Color.from_names(('blue', 'green', 'yellow', 'red', 'magenta'))
RESET = colors.Color.reset


# named logging levels
DEBUG    = LEVELS[0]
INFO     = LEVELS[1]
WARNING  = LEVELS[2]
ERROR    = LEVELS[3]
CRITICAL = LEVELS[4]
LEVELS_BY_NAME = {'DEBUG': DEBUG, 'INFO': INFO, 'WARNING': WARNING,
                  'ERROR': ERROR, 'CRITICAL': CRITICAL}


# NOTE: global handler list lets `Logger` instances aware of changes
#       to other logger's handlers. (i.e., changing from StandardHandler to DetailedHandler).
_handlers: List[handlers.Handler] = []


@dataclass
class Message(messages.Message):
    """Message data class (level, content, timestamp, topic, source, host)."""
    level: levels.Level
    content: Any
    topic: str
    time: datetime
    host: str = HOSTNAME


class Logger(loggers.Logger):
    """Logger for StreamKit."""

    levels = LEVELS
    colors = COLORS

    topic: str = 'streamkit'
    Message: type = Message
    callbacks: dict = {'time': datetime.now, }

    def __init__(self, topic: str) -> None:
        """Setup logger with custom callback for `topic`."""
        super().__init__()
        self.topic = topic
        self.callbacks = {**self.callbacks, 'topic': (lambda: topic)}

    @property
    def handlers(self) -> List[handlers.Handler]:
        """Override of local handlers to global list."""
        global _handlers
        return _handlers

    # NOTE: dynamic instrumentation makes linters sad
    debug: Callable
    info: Callable
    warning: Callable
    error: Callable
    critical: Callable


@dataclass
class StandardHandler(handlers.Handler):
    """Write basic colorized messages to standard error."""

    level: levels.Level
    resource: io.TextIOWrapper = sys.stderr

    def format(self, msg: Message) -> str:
        """Colorize the log level and with only the message."""
        COLOR = Logger.colors[msg.level.value].foreground
        return f'{COLOR}{msg.level.name:<8}{RESET} {msg.topic}: {msg.content}'


@dataclass
class DetailedHandler(handlers.Handler):
    """Write detailed (syslog-like) messages to standard error."""

    level: levels.Level
    resource: io.TextIOWrapper = sys.stderr

    def format(self, msg: Message) -> str:
        """Syslog style with padded spaces."""
        ts = msg.time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return f'{ts} {msg.host} {msg.level.name:<8} [{msg.topic}] {msg.content}'


# persistent instances (STANDARD_HANDLER is the default)
STANDARD_HANDLER = StandardHandler(WARNING)
DETAILED_HANDLER = DetailedHandler(WARNING)
_handlers.append(STANDARD_HANDLER)


HANDLERS_BY_NAME = {'STANDARD': STANDARD_HANDLER,
                    'DETAILED': DETAILED_HANDLER}
