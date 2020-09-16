# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Subscriber interface for StreamKit."""

# type annotations
from __future__ import annotations
from typing import List, Iterator, Optional

# standard libs
from datetime import datetime
from time import sleep
from threading import Thread
from queue import Queue, Empty

# internal libs
from .core.logging import Logger
from .database.message import Message, fetch
from .database.access import latest, update
from .database.core.session import Session


# module level logger
log = Logger(__name__)

# shared parameters
DEFAULT_BATCHSIZE: int = 10
DEFAULT_TIMEOUT: Optional[float] = None
DEFAULT_POLL: float = 1


class Subscription(Thread):
    """Query for new messages for a given `topic` and enqueue them."""

    queue: Queue = None
    topic: str = None
    subscriber: str = None
    poll: float = None
    batchsize: int = None
    terminated: bool = False

    def __init__(self, queue: Queue, topic: str, subscriber: str,
                 batchsize: int = DEFAULT_BATCHSIZE, poll: float = DEFAULT_POLL) -> None:
        """Initialize thread."""
        self.poll = poll
        self.queue = queue
        self.topic = topic
        self.batchsize = batchsize
        self.subscriber = subscriber
        super().__init__(daemon=True)

    def run(self) -> None:
        """Poll database for new messages."""
        session = Session()  # NOTE: thread-local session
        time = latest(self.subscriber, self.topic, session).time
        log.debug(f'starting {self.topic}-thread (latest: {time})')
        while not self.terminated:
            start = datetime.now()
            last = time
            try:
                messages = fetch(self.topic, last, self.batchsize, session)
                log.debug(f'received {len(messages)} messages [topic={self.topic}]')
                for msg in messages:
                    if not self.terminated:
                        self.queue.put(msg)
                        last = msg.time
                    else:
                        break
            finally:
                if last > time:
                    update(self.subscriber, self.topic, last, session)
                    log.debug(f'updated {self.subscriber}:{self.topic} (latest: {last})')
                    time = last

            elapsed = (datetime.now() - start).total_seconds()
            remaining = self.poll - elapsed
            wait_period = remaining if remaining > 0 else 0
            sleep(wait_period)

    def terminate(self) -> None:
        """Signal to shut down the thread."""
        log.debug(f'stopping {self.subscriber}:{self.topic}')
        self.terminated = True


class Subscriber:
    """
    A Subscriber defines the interface for awaiting messages on a given topic.

    Example:
        >>> with Subscriber('my_sub', topics=['example']) as stream:
        ...     for message in stream:
        ...         print(f'{message.topic}: {message.text}')
    """

    name: str = None
    queue: Queue = None
    topics: List[str] = None
    threads: List[Subscription] = None
    poll: Optional[float] = None

    def __init__(self, name: str, topics: List[str], batchsize: int = DEFAULT_BATCHSIZE,
                 poll: float = DEFAULT_POLL, timeout: float = DEFAULT_TIMEOUT) -> None:
        """
        Initialize subscriber threads.

        Args:
            name (str):
                Unique name for this subscriber.
            topics (List[str]):
                List of topic names.
            batchsize (int):
                Maximum number of messages to return in a batch.
            poll (float):
                Seconds to wait in between database queries.
            timeout (float):
                Timeout in seconds if no messages.
                Default of None implies no timeout.
        """

        self.name = name
        self.poll = poll
        self.topics = topics
        self.queue = Queue(maxsize=len(self.topics)*batchsize)
        self.timeout = timeout
        self.threads = [Subscription(self.queue, topic, name, batchsize, poll)
                        for topic in self.topics]

    def start(self) -> None:
        """Start subscription threads."""
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        """Terminate all threads."""
        for thread in self.threads:
            thread.terminate()

    def __enter__(self) -> Subscriber:
        """Start all threads."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Join all threads and stop."""
        self.stop()

    def __iter__(self) -> Iterator[Message]:
        """Yield messages from the queue."""
        yield from iter(self.get_message, None)

    def get_message(self, timeout: float = None) -> Optional[Message]:
        """
        Get next message from queue.

        Args:
            timeout (float):
                Seconds until timeout if no message.
                If None, default to timeout given at initialization.

        Returns:
            message (Message):
                The next message for the given topics.
        """
        timeout = None if timeout is None else float(timeout)
        timeout = timeout if timeout is not None else self.timeout
        try:
            return self.queue.get(timeout=timeout)
        except Empty:
            log.info('timeout reached')
            return None
