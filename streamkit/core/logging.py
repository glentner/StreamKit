# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Logging configuration for StreamKit."""


# standard libraries
import socket
import logging as _logging

# internal libs
from .config import config


# cached for frequent use
_HOST = socket.gethostname()


# escape sequences
_ANSI_RESET = '\033[0m'
_ANSI_CODES = {prefix: {color: '\033[{prefix}{num}m'.format(prefix=i + 3, num=j)
                        for j, color in enumerate(['black', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan', 'white'])}
               for i, prefix in enumerate(['foreground', 'background'])}
_LEVEL_COLORS = {'debug': 'blue', 'info': 'green', 'warning': 'yellow',
                 'error': 'red', 'critical': 'magenta'}


class LogRecord(_logging.LogRecord):
    """Extends :class:`logging.LogRecord` to include a hostname and ANSI color codes."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hostname = _HOST
        self.ansi_color = _ANSI_CODES['foreground'][_LEVEL_COLORS[self.levelname.lower()]]
        self.ansi_reset = _ANSI_RESET


# inject factory back into logging library
_logging.setLogRecordFactory(LogRecord)

# configuration
def initialize_logging() -> None:
    """Configure with func:`logging.basicConfig` for command-line."""
    _logging.basicConfig(level=getattr(_logging, config['logging']['level'].upper()),
                         format=config['logging']['format'],
                         datefmt=config['logging']['datefmt'])
