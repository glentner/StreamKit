# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Access application layer for StreamKit."""


# standard libs
from datetime import datetime
import logging

# external libs
from sqlalchemy.orm.exc import NoResultFound

# internal libs
from .core.session import Session
from .core.orm import Message, Access
from .core.keys import get_topic, get_subscriber


# initialize module level logger
log = logging.getLogger(__name__)


def latest(subscriber: str, topic: str, session: Session = None) -> Access:
    """
    Query for latest access record for the given `subscriber` and `topic`.
    If no previous access exists, the timestamp of the most recent message
    on the `topic` will be used to create a new access record. If no message
    exists on this topic, the current timestamp is used.

    Args:
        subscriber (str):
            Name of the subscriber.
        topic (str):
            Name of the topic.
        session (:class:`Session`):
            An existing session (optional).

    Returns:
        latest (:class:`Access`):
            Latest access record.
    """
    session = session or Session()
    subscriber_id = get_subscriber(subscriber, session).id
    topic_id = get_topic(topic, session).id
    try:
        return (session.query(Access).
                filter_by(subscriber_id=subscriber_id).
                filter_by(topic_id=topic_id)).one()
    except NoResultFound:
        # latest message on the topic
        most_recent = (session.query(Message).
                       filter_by(topic_id=topic_id).
                       order_by(Message.time)).first()
        if most_recent is None:
            new_access = Access(subscriber_id=subscriber_id, topic_id=topic_id,
                                time=datetime.now())
        else:
            new_access = Access(subscriber_id=subscriber_id, topic_id=topic_id,
                                time=most_recent.time)
        session.add(new_access)
        session.commit()
        return new_access


def update(subscriber: str, topic: str, time: datetime, session: Session = None) -> None:
    """
    Update the access record for the given `subscriber` on a
    `topic` with the most recent message `id` received.

    Args:
        subscriber (str):
            Name of subscriber.
        topic (str):
            Name of topic.
        time (`datetime`):
            The timestamp of the most recently handled message.
        session (`Session`):
            An existing session (optional).
    """
    session = session or Session()
    subscriber_id = get_subscriber(subscriber, session).id
    topic_id = get_topic(topic, session).id
    try:
        query = (session.query(Access).
                 filter_by(subscriber_id=subscriber_id).
                 filter_by(topic_id=topic_id))
        access = query.one()
        access.time = time
        session.commit()
    except NoResultFound:
        access = Access(subscriber_id=subscriber_id,
                        topic_id=topic_id, time=time)
        session.add(access)
        session.commit()

