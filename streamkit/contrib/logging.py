# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""StreamKitHandler implementation."""


# standard libs
from logging import Handler, LogRecord

# internal libs
from ..publisher import Publisher, DEFAULT_TIMEOUT, DEFAULT_BATCHSIZE


class StreamKitHandler(Handler):
    """Publish records using StreamKit."""

    publisher: Publisher = None

    def __init__(self, batchsize: int = DEFAULT_BATCHSIZE, timeout: float = DEFAULT_TIMEOUT) -> None:
        """Initialize publisher."""
        super().__init__()
        self.publisher = Publisher(batchsize=batchsize, timeout=timeout)
        self.publisher.start()

    def emit(self, record: LogRecord) -> None:
        """Publish `record`."""
        self.publisher.write(record.msg, topic=record.name, level=record.levelname)

    def close(self) -> None:
        """
        Disconnect and shutdown publishing thread.
        Automatically called by the :mod:`logging` module.
        """
        self.publisher.stop()
