# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Database ORM for StreamKit."""


# type annotations
from __future__ import annotations
from typing import Tuple, Dict, Any

# standard libs
import logging

# external libs
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Index, Integer, BigInteger, String, DateTime
from sqlalchemy.schema import Sequence, CheckConstraint
from sqlalchemy.orm import relationship

# internal libs
from .engine import schema, db_config


# initialize module level logger
log = logging.getLogger(__name__)


Table = declarative_base()


class Level(Table):
    """
    A level relates a name and its identifier.
    """

    __tablename__ = 'level'
    __table_args__ = {'schema': schema}
    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True, nullable=False)

    def __repr__(self) -> str:
        return f'<Level(id={repr(self.id)}, name={repr(self.name)})>'

    def to_dict(self) -> Dict[str, Any]:
        """Python dictionary representation."""
        return {'id': self.id, 'name': self.name}

    def values(self, joined: bool = False) -> Tuple:  # noqa: unused parameter
        """
        Tuple representation of values.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Level`.

        Returns:
            row (tuple):
                The `Level.id` and `Level.name`.
        """
        return self.id, self.name

    @staticmethod
    def keys(joined: bool = False) -> Tuple[str, ...]:  # noqa: unused parameter
        """
        Tuple representation of keys.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Level`.

        Returns:
            keys (tuple): ('id', 'name')
        """
        return 'id', 'name'

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        return {'level_id': self.id, 'level_name': self.name}


class Topic(Table):
    """
    A topic relates a name and its identifier.
    """

    __tablename__ = 'topic'
    __table_args__ = {'schema': schema}
    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True, nullable=False)

    def __repr__(self) -> str:
        return f'<Topic(id={repr(self.id)}, name={repr(self.name)})>'

    def to_dict(self) -> Dict[str, Any]:
        """Python dictionary representation."""
        return {'id': self.id, 'name': self.name}

    def values(self, joined: bool = False) -> Tuple:  # noqa: unused parameter
        """
        Tuple representation of values.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Topic`.

        Returns:
            row (tuple):
                The `Topic.id` and `Topic.name`.
        """
        return self.id, self.name

    @staticmethod
    def keys(joined: bool = False) -> Tuple[str, ...]:  # noqa: unused parameter
        """
        Tuple representation of keys.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Topic`.

        Returns:
            keys (tuple): ('id', 'name')
        """
        return 'id', 'name'

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        return {'topic_id': self.id, 'topic_name': self.name}


class Host(Table):
    """
    A host relates a name and its identifier.
    """

    __tablename__ = 'host'
    __table_args__ = {'schema': schema}
    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True, nullable=False)

    def __repr__(self) -> str:
        return f'<Host(id={repr(self.id)}, name={repr(self.name)})>'

    def to_dict(self) -> Dict[str, Any]:
        """Python dictionary representation."""
        return {'id': self.id, 'name': self.name}

    def values(self, joined: bool = False) -> Tuple:  # noqa: unused parameter
        """
        Tuple representation of values.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Host`.

        Returns:
            row (tuple):
                The `Host.id` and `Host.name`.
        """
        return self.id, self.name

    @staticmethod
    def keys(joined: bool = False) -> Tuple[str, ...]:  # noqa: unused parameter
        """
        Tuple representation of keys.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Host`.

        Returns:
            keys (tuple): ('id', 'name')
        """
        return 'id', 'name'

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        return {'host_id': self.id, 'host_name': self.name}


class Message(Table):

    __tablename__ = 'message'
    __table_args__ = {'schema': schema}

    id = Column('id', BigInteger().with_variant(Integer, 'sqlite'), primary_key=True)
    time = Column('time', DateTime(timezone=True), nullable=False)
    topic_id = Column('topic_id', Integer, ForeignKey(Topic.id), nullable=False)
    level_id = Column('level_id', Integer, ForeignKey(Level.id), nullable=False)
    host_id = Column('host_id', Integer, ForeignKey(Host.id), nullable=False)
    text = Column('text', String, nullable=False)

    topic = relationship('Topic', backref='message')
    level = relationship('Level', backref='message')
    host  = relationship('Host', backref='message')

    # conditionally redefine for time-based partitioning
    if db_config['backend'] in ('timescale', ):
        # NOTE: The primary key is (`time`, `topic_id`) NOT `id`.
        # This is weird but important for automatic hyper-table partitioning
        # on the `time` values for TimeScaleDB (PostgreSQL).
        id = Column('id', BigInteger,
                    Sequence('message_id_seq', start=1, increment=1, schema=schema),
                    CheckConstraint('id > 0', name='message_id_check'), nullable=False)

        time = Column('time', DateTime(timezone=True), nullable=False, primary_key=True)
        topic_id = Column('topic_id', Integer, nullable=False, primary_key=True)

        # NOTE: remove explicit foreign keys?
        # level_id = Column('level_id', Integer, nullable=False)
        # host_id = Column('host_id', Integer, nullable=False)
        #
        # topic = relationship('Topic', backref='message', foreign_keys=[topic_id],
        #                      primaryjoin='Message.topic_id == Topic.id')
        # level = relationship('Level', backref='message', foreign_keys=[level_id],
        #                      primaryjoin='Message.level_id == Level.id')
        # host  = relationship('Host', backref='message', foreign_keys=[host_id],
        #                      primaryjoin='Message.host_id == Host.id')

    def __repr__(self) -> str:
        return (f'<Message(id={repr(self.id)},'
                f' time={repr(self.time)},'
                f' topic_id={repr(self.topic_id)},'
                f' level_id={repr(self.level_id)},'
                f' host_id={repr(self.host_id)},'
                f' text={repr(self.text)})>')

    def to_dict(self) -> Dict[str, Any]:
        """Python dictionary representation."""
        return {'id': self.id, 'time': self.time, 'topic_id': self.topic_id,
                'level_id': self.level_id, 'host_id': self.host_id, 'text': self.text}

    def values(self, joined: bool = False) -> Tuple:
        """
        Tuple representation of values.

        Args:
            joined (bool):
                Join relations instead of identifiers (default: False).
        """
        if not joined:
            return self.id, self.time, self.topic_id, self.level_id, self.host_id, self.text
        else:
            return self.id, self.time, self.topic.name, self.level.name, self.host.name, self.text

    @staticmethod
    def keys(joined: bool = False) -> Tuple[str, ...]:
        """
        Tuple representation of keys.

        Args:
            joined (bool):
                Join relations instead of identifiers (default: False).
        """
        if not joined:
            return 'id', 'time', 'topic_id', 'level_id', 'host_id', 'text'
        else:
            return 'id', 'time', 'topic', 'level', 'host', 'text'

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        message = {'message_time': self.time.strftime('%Y-%m-%d %H:%M:%S.%f'),
                   'message_text': self.text}
        return {**message, **self.topic.embedded(),
                **self.level.embedded(), **self.host.embedded()}


if db_config['backend'] in ('timescale', ):
    # we use time-topic PK and need to index ID
    message_id_index = Index('message_id_index', Message.id)
else:
    message_time_topic_index = Index('message_time_topic_index', Message.time, Message.topic_id)


# efficient filtering on host or level
message_level_index = Index('message_level_index', Message.level_id)
message_host_index = Index('message_host_index', Message.host_id)


class Subscriber(Table):
    """
    A subscriber relates a name and its identifier.
    """

    __tablename__ = 'subscriber'
    __table_args__ = {'schema': schema}
    id = Column('id', Integer, primary_key=True)
    name = Column('name', String, unique=True, nullable=False)

    def __repr__(self) -> str:
        return f'<Subscriber(id={repr(self.id)}, name={repr(self.name)})>'

    def to_dict(self) -> Dict[str, Any]:
        """Python dictionary representation."""
        return {'id': self.id, 'name': self.name}

    def values(self, joined: bool = False) -> Tuple:  # noqa: unused parameter
        """
        Tuple representation of values.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Subscriber`.

        Returns:
            row (tuple):
                The `Subscriber.id` and `Subscriber.name`.
        """
        return self.id, self.name

    @staticmethod
    def keys(joined: bool = False) -> Tuple[str, ...]:  # noqa: unused parameter
        """
        Tuple representation of keys.

        Args:
            joined (bool):
                Join relations instead of identifiers.
                Not used by `Subscriber`.

        Returns:
            keys (tuple): ('id', 'name')
        """
        return 'id', 'name'

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        return {'subscriber_id': self.id, 'subscriber_name': self.name}


class Access(Table):

    __tablename__ = 'access'
    __table_args__ = {'schema': schema}
    subscriber_id = Column('subscriber_id', Integer, ForeignKey(Subscriber.id), nullable=False, primary_key=True)
    topic_id = Column('topic_id', Integer, ForeignKey(Topic.id), nullable=False, primary_key=True)
    time = Column('time', DateTime(timezone=True), nullable=False)

    subscriber = relationship('Subscriber', backref='access')
    topic = relationship('Topic', backref='access')

    def __repr__(self) -> str:
        return (f'<Access(subscriber_id={repr(self.subscriber_id)},'
                f' topic_id={repr(self.topic_id)},'
                f' time={repr(self.time)})>')

    def to_dict(self) -> Dict[str, Any]:
        """Python dictionary representation."""
        return {'subscriber_id': self.subscriber_id,
                'topic_id': self.topic_id,
                'time': self.time}

    def values(self, joined: bool = False) -> Tuple:
        """
        Tuple representation of values.

        Args:
            joined (bool):
                Join relations instead of identifiers (default: False).
        """
        if not joined:
            return self.subscriber_id, self.topic_id, self.time
        else:
            return self.subscriber.name, self.topic.name, self.time

    @staticmethod
    def keys(joined: bool = False) -> Tuple[str, ...]:
        """
        Tuple representation of keys.

        Args:
            joined (bool):
                Join relations instead of identifiers (default: False).
        """
        if not joined:
            return 'subscriber_id', 'topic_id', 'time'
        else:
            return 'subscriber', 'topic', 'time'

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        access = {'access_time': self.time.strftime('%Y-%m-%d %H:%M:%S.%f')}
        return {**access, **self.subscriber.embedded(), **self.topic.embedded()}


tables: Dict[str, Table] = {
    'level': Level,
    'topic': Topic,
    'host': Host,
    'message': Message,
    'subscriber': Subscriber,
    'access': Access
}
