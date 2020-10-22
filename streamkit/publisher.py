# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Publisher interface for StreamKit."""


# type annotations
from __future__ import annotations
from typing import Optional

# standard libs
import logging
from threading import Thread
from queue import Queue, Empty

# internal libs
from .database.message import Message, publish


# initialize module level logger
log = logging.getLogger(__name__)


# shared parameters
DEFAULT_BATCHSIZE: int = 10
DEFAULT_TIMEOUT: float = 5.0


class QueueThread(Thread):
    """Enqueue and flush messages to the database."""

    queue: Queue = None
    timeout: float = None
    batchsize: int = None
    terminated: bool = False

    def __init__(self, queue: Queue, batchsize: int = DEFAULT_BATCHSIZE,
                 timeout: float = DEFAULT_TIMEOUT) -> None:
        """Initialize publishing thread."""
        self.queue = queue
        self.timeout = float(timeout)
        self.batchsize = batchsize
        super().__init__(daemon=True)

    def run(self) -> None:
        """Get messages from the queue and publish to the database."""
        log.debug('starting publisher-thread')
        messages = []
        while not self.terminated:
            try:
                messages.clear()
                for count in range(self.batchsize):
                    if not self.terminated:
                        message = Message(**self.queue.get(timeout=self.timeout))
                        messages.append(message)
            except Empty:
                pass
            finally:
                if messages:
                    publish(messages)
                    log.info(f'added {len(messages)} messages')
                    for count, _ in enumerate(messages):
                        self.queue.task_done()

    def terminate(self) -> None:
        """Signal to shut down the thread."""
        log.debug('stopping publisher-thread')
        self.terminated = True


class Publisher:
    """
    A Publisher defines the interface for writing messages to the stream.

    Example:
        >>> with Publisher(batchsize=10) as stream:
        ...     stream.write('hello, world!', topic='example', level='INFO')
    """

    queue: Queue = None
    topic: str = None
    thread: QueueThread = None
    batchsize: Optional[int] = DEFAULT_BATCHSIZE
    timeout: Optional[float] = DEFAULT_TIMEOUT

    def __init__(self, topic: str = None, level: str = None,
                 batchsize: int = DEFAULT_BATCHSIZE, timeout: float = DEFAULT_TIMEOUT) -> None:
        """
        Initialize publisher.

        Args:
            topic (str):
                Default topic name (optional).
            level (str):
                Default level name (optional).
            batchsize (int):
                Number of messages to accumulate before committing.
                Default to `DEFAULT_BATCHSIZE`.
            timeout (float):
                Seconds to wait on new messages before committing.
                Default to `DEFAULT_TIMEOUT`.
        """
        self.topic = None if topic is None else str(topic)
        self.level = None if level is None else str(level)
        self.queue = Queue(maxsize=2*batchsize)
        self.timeout = float(timeout)
        self.thread = QueueThread(queue=self.queue, batchsize=batchsize, timeout=self.timeout)

    def start(self) -> None:
        """Start subscription threads."""
        self.thread.start()

    def stop(self) -> None:
        """Terminate all threads."""
        self.queue.join()
        self.thread.terminate()
        self.thread.join()

    def __enter__(self) -> Publisher:
        """Start all threads."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Join all threads and stop."""
        self.stop()

    def write(self, text: str, level: str = None, topic: str = None) -> None:
        """
        Publish a message.

        Args:
            text:
                Message text.
            topic:
                Message topic (optional if specified globally).
            level:
                Message level (optional if specified globally).
        """
        self.queue.put({'text': str(text),
                        'topic': self.topic if topic is None else str(topic),
                        'level': self.level if level is None else str(level)})
