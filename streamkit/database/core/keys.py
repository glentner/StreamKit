# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Database primary key lookup, creation, and caching for StreamKit."""


# standard libs
import functools

# external libs
from sqlalchemy.orm.exc import NoResultFound

# internal libs
from ...core.logging import Logger
from .session import Session
from .orm import Table, Level, Topic, Host, Subscriber

# module level logger
log = Logger(__name__)


def __get(table: type, name: str, session: Session = None) -> Table:
    """
    Fetch an existing record of `table` type (id, name).
    If none exists, create one and return it with a new id.

    Args:
        table (type):
            The declarative table class to lookup.
        name (str):
            The name that corresponds to the id.
        session (Session):
            Optionally specify an existing session.

    Returns:
        record:
            The instance of the record with the id.
    """
    session = session if session is not None else Session()
    try:
        record = session.query(table).filter_by(name=name).one()
        return record
    except NoResultFound:
        pass
    record = table(name=name)
    session.add(record)
    session.commit()
    return table(id=record.id, name=record.name)  # NOTE: enforces attribute refresh


@functools.lru_cache
def get_level(name: str, session: Session = None) -> Level:
    """
    Fetch the existing `Level` record. Creates a new one if necessary.
    The record is cached in-memory after the first call with `name`.

    Args:
        name (str):
            The name of the level.
        session (Session):
            Optionally specify an existing session.

    Returns:
        level (Level):
            The fetched level.
    """
    return __get(Level, name, session)


@functools.lru_cache
def get_topic(name: str, session: Session = None) -> Topic:
    """
    Fetch the existing `Topic` record. Creates a new one if necessary.
    The record is cached in-memory after the first call with `name`.

    Args:
        name (str):
            The name of the topic.
        session (Session):
            Optionally specify an existing session.

    Returns:
        topic (Topic):
            The fetched topic.
    """
    return __get(Topic, name, session)


@functools.lru_cache
def get_host(name: str, session: Session = None) -> Host:
    """
    Fetch the existing `Host` record. Creates a new one if necessary.
    The record is cached in-memory after the first call with `name`.

    Args:
        name (str):
            The name of the host.
        session (Session):
            Optionally specify an existing session.

    Returns:
        host (Host):
            The fetched host.
    """
    return __get(Host, name, session)


@functools.lru_cache
def get_subscriber(name: str, session: Session = None) -> Subscriber:
    """
    Fetch the existing `Subscriber` record. Creates a new one if necessary.
    The record is cached in-memory after the first call with `name`.

    Args:
        name (str):
            The name of the subscriber.
        session (Session):
            Optionally specify an existing session.

    Returns:
        subscriber (Subscriber):
            The fetched subscriber.
    """
    return __get(Subscriber, name, session)
