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
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, ForeignKey, Index, Integer, String, DateTime

# internal libs
from .engine import schema
from ...core.logging import Logger


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

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        return {'host_id': self.id, 'host_name': self.name}


class Message(Table):

    __tablename__ = 'message'
    __table_args__ = {'schema': schema}

    time = Column('time', DateTime(timezone=True), nullable=False, primary_key=True)
    topic_id = Column('topic_id', Integer, ForeignKey(Topic.id), nullable=False, primary_key=True)

    level_id = Column('level_id', Integer, ForeignKey(Level.id), nullable=False)
    host_id = Column('host_id', Integer, ForeignKey(Host.id), nullable=False)
    text = Column('text', String, nullable=False)

    topic = relationship('Topic', backref='message')
    level     = relationship('Level', backref='message')
    host      = relationship('Host', backref='message')

    def __repr__(self) -> str:
        return (f'<Message(time={repr(self.time)},'
                f' topic_id={repr(self.topic_id)},'
                f' text={repr(self.text)},'
                f' level_id={repr(self.level_id)},'
                f' host_id={repr(self.host_id)}>')

    def to_dict(self) -> Dict[str, Any]:
        """Python dictionary representation."""
        return {'time': self.time, 'topic_id': self.topic_id, 'level_id': self.level_id,
                'host_id': self.host_id, 'text': self.text}

    def embedded(self) -> Dict[str, Any]:
        """Re-map keys and include join relationships."""
        message = {'message_time': self.time.strftime('%Y-%m-%d %H:%M:%S.%f'),
                   'message_text': self.text}
        return {**message, **self.topic.embedded(),
                **self.level.embedded(), **self.host.embedded()}


# additional indices on message table
Message_LevelIndex = Index('index_message_level', Message.level_id)
Message_HostIndex = Index('index_message_host', Message.host_id)


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
