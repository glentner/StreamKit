# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Message application layer for StreamKit."""


# type annotations
from __future__ import annotations
from typing import List, Dict, Any

# standard libs
from datetime import datetime
import socket
import logging

# external libs
from sqlalchemy.orm import joinedload

# internal libs
from .core.orm import Message as _Message
from .core.keys import get_level, get_topic, get_host
from .core.session import Session


# initialize module level logger
log = logging.getLogger(__name__)


# single global instance
HOST = socket.gethostname()


class Message:
    """
    A message associates content with metadata about its origin and context.
    """
    id: int = None
    time: datetime = None
    topic: str = None
    level: str = None
    host: str = None
    text: str = None

    def __init__(self, **fields) -> None:
        """Initialize directly from `fields`."""
        try:
            self.id = fields.pop('id', None)
            self.time = fields.pop('time', datetime.utcnow())
            self.topic = fields.pop('topic')
            self.level = fields.pop('level')
            self.host = fields.pop('host', HOST)
            self.text = fields.pop('text')
        except KeyError as field:
            raise AttributeError(f'Message.{field} is required.') from field
        else:
            for field in fields:
                raise AttributeError(f'Message.{field}')

    def __repr__(self) -> str:
        return (f'Message(id={repr(self.id)}, time={repr(self.time)}, topic={repr(self.topic)}, '
                f'level={repr(self.level)}, host={repr(self.host)}, text={repr(self.text)})')

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {'id': self.id, 'time': self.time, 'topic': self.topic, 'level': self.level,
                'host': self.host, 'text': self.text}

    def to_record(self, session: Session = None) -> _Message:
        """
        Convert to ORM Message.

        Args:
            session (`Session`):
                An existing session (optional).

        Returns:
            message (`_Message`):
                The message record (ORM).
        """
        session = session or Session()
        return _Message(id=self.id,
                        time=self.time,
                        topic_id=get_topic(self.topic, session).id,
                        level_id=get_level(self.level, session).id,
                        host_id=get_host(self.host, session).id,
                        text=self.text)


def publish(messages: List[Message], session: Session = None) -> None:
    """
    Add all `messages` to the database.

    Args:
        messages (List[Message]):
            List of Message instances.
        session (Session):
            Existing session (optional).
    """
    session = session or Session()
    session.add_all([msg.to_record() for msg in messages])
    session.commit()


def fetch(topic: str, time: datetime, limit: int, session: Session = None) -> List[Message]:
    """
    Query for messages newer than `id` on `topic`.
    Returns at most, `limit` number of messages.

    Args:
        time (datetime):
            Timestamp of the last message received.
        topic (str):
            Name of topic.
        limit (int):
            Limit on the number of returned messages.
        session (Session):
            Existing session (optional).

    Returns:
        messages (List[Message]):
            List of messages.
    """
    session = session or Session()
    return [Message(id=msg.id, time=msg.time, topic=msg.topic.name,
                    level=msg.level.name, host=msg.host.name, text=msg.text)
            for msg in (session.query(_Message).
                        options(joinedload(_Message.topic)).
                        options(joinedload(_Message.level)).
                        options(joinedload(_Message.host)).
                        filter(_Message.time > time).
                        filter(_Message.topic_id == get_topic(topic, session).id).
                        order_by(_Message.time).
                        limit(limit))]

