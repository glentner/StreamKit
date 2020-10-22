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
from typing import List, Dict, Iterator, Optional

# standard libs
import logging
from datetime import datetime
from time import sleep
from threading import Thread
from queue import Queue, Empty

# internal libs
from .database.message import Message, fetch
from .database.access import latest, update
from .database.core.session import Session
from .database.core.orm import Topic


# initialize module level logger
log = logging.getLogger(__name__)


# shared parameters
DEFAULT_BATCHSIZE: int = 10
DEFAULT_TIMEOUT: Optional[float] = None
DEFAULT_POLL: float = 1


# topics are conventionally separated by a single `.` character
# as in programming language modules and packages (e.g., __name__ in python).
# override this by re-assigning to a different character(s)
TOPIC_SEP: str = '.'


# sentinel value to signal stop iteration
STOP: int = -1


class TopicThread(Thread):
    """Query for new messages for a given `topic` and enqueue them."""

    queue: Queue = None
    topic: str = None
    subscriber: str = None
    poll: float = None
    batchsize: int = None
    terminated: bool = False

    def __init__(self, subscriber: str, topic: str, queue: Queue,
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
        log.debug(f'starting topic-thread ({self.subscriber}:{self.topic}, {time})')

        while not self.terminated:
            start = datetime.now()
            last = time
            try:
                messages = fetch(self.topic, last, self.batchsize, session)
                if messages:
                    log.debug(f'received {len(messages)} messages ({self.subscriber}:{self.topic})')
                for msg in messages:
                    if not self.terminated:
                        self.queue.put(msg)
                        last = msg.time
                    else:
                        break
            finally:
                if last > time:
                    update(self.subscriber, self.topic, last, session)
                    log.debug(f'updated ({self.subscriber}:{self.topic}, {last})')
                    time = last
                session.commit()

            elapsed = (datetime.now() - start).total_seconds()
            remaining = self.poll - elapsed
            wait_period = remaining if remaining > 0 else 0
            sleep(wait_period)

    def terminate(self) -> None:
        """Signal to shut down the thread."""
        log.debug(f'stopping topic-thread ({self.subscriber}:{self.topic})')
        self.terminated = True


class NameThread(Thread):
    """Query for new topics given named `roots`."""

    queue: Queue = None
    roots: List[str] = None

    poll: float = None
    terminated: bool = False
    subscriber: str = None

    def __init__(self, subscriber: str, roots: List[str], queue: Queue,
                 poll: float = DEFAULT_POLL) -> None:
        """Initialize thread."""
        self.poll = poll
        self.queue = queue
        self.roots = roots
        self.subscriber = subscriber
        super().__init__(daemon=True)

    def run(self) -> None:
        """Poll database for new topics."""

        log.debug(f'starting name-thread ({self.subscriber})')
        session = Session()  # NOTE: thread-local session

        while not self.terminated:
            start = datetime.now()
            for name in self.roots:
                self.queue.put(name)
                for subtopic in session.query(Topic).filter(Topic.name.like(f'{name}{TOPIC_SEP}%')):
                    self.queue.put(subtopic.name)
                    if self.terminated:
                        break
                if self.terminated:
                    break
            session.commit()
            if self.terminated:
                break

            elapsed = (datetime.now() - start).total_seconds()
            remaining = self.poll - elapsed
            wait_period = remaining if remaining > 0 else 0
            sleep(wait_period)

    def terminate(self) -> None:
        """Signal to shut down the thread."""
        log.debug(f'stopping name-thread ({self.subscriber})')
        self.terminated = True
        self.queue.put(STOP)


class ManagerThread(Thread):
    """Manage name thread and topic threads."""

    topics: List[str] = None

    topic_queue: Queue = None
    message_queue: Queue = None

    poll: float = None
    batchsize: int = None
    terminated: bool = False
    subscriber: str = None

    topic_threads: Dict[str, TopicThread] = None
    name_thread: NameThread = None

    def __init__(self, subscriber: str, topics: List[str], topic_queue: Queue, message_queue: Queue,
                 batchsize: int = DEFAULT_BATCHSIZE, poll: float = DEFAULT_POLL) -> None:
        """Initialize thread."""
        self.poll = poll
        self.topics = topics
        self.subscriber = subscriber
        self.topic_queue = topic_queue
        self.message_queue = message_queue
        self.batchsize = batchsize
        super().__init__(daemon=True)

    def run(self) -> None:
        """Poll database for new topics and spawn threads."""

        log.debug(f'starting manager-thread ({self.subscriber})')
        self.name_thread = NameThread(self.subscriber, self.topics, self.topic_queue, self.poll)
        self.name_thread.start()

        previous = datetime.now()
        self.topic_threads = dict()
        for topic in iter(self.topic_queue.get, STOP):
            if topic not in self.topic_threads:
                thread = TopicThread(self.subscriber, topic, self.message_queue, self.batchsize, self.poll)
                thread.start()
                self.topic_threads[topic] = thread
                self.topic_queue.task_done()

                # NOTE: add small spacing between thread start or collisions may occur
                elapsed = (datetime.now() - previous).total_seconds()
                remaining = 0.5 - elapsed
                wait_period = remaining if remaining > 0 else 0
                sleep(wait_period)
                previous = datetime.now()

    def terminate(self) -> None:
        """Signal to shut down the thread."""
        log.debug(f'stopping manager-thread ({self.subscriber})')
        self.terminated = True
        self.name_thread.terminate()
        self.name_thread.join()
        for topic, thread in self.topic_threads.items():
            thread.terminate()
            thread.join()


class Subscriber:
    """
    A Subscriber defines the interface for awaiting messages on a given topic.

    Example:
        >>> with Subscriber('<name>', topics=['<topic_1>', '<topic_2>']) as stream:
        ...     for message in stream:
        ...         print(f'{message.topic}: {message.text}')
    """

    name: str = None
    topics: List[str] = None
    poll: Optional[float] = None
    timeout: float = None
    batchsize: int = None

    topic_queue: Queue = None
    message_queue: Queue = None
    manager_thread: ManagerThread = None

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
        self.timeout = timeout
        self.batchsize = batchsize
        self.topic_queue = Queue(maxsize=10)  # NOTE: arbitrary choice
        self.message_queue = Queue(maxsize=len(self.topics)*batchsize)
        self.manager_thread = ManagerThread(subscriber=self.name, topics=self.topics, topic_queue=self.topic_queue,
                                            message_queue=self.message_queue, batchsize=self.batchsize,
                                            poll=self.poll)

    def start(self) -> None:
        """Start manager thread."""
        self.manager_thread.start()

    def stop(self) -> None:
        """Terminate all threads."""
        self.manager_thread.terminate()
        self.manager_thread.join()

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
            message = self.message_queue.get(timeout=timeout)
            self.message_queue.task_done()
            return message
        except Empty:
            log.info(f'timeout reached ({self.name})')
            return None
