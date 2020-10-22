# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Dump contents of database."""


# type annotations
from __future__ import annotations
from typing import Tuple, List, Dict, Optional, Union, Callable, IO

# standard libs
import sys
import functools
import logging

# internal libs
from ...core.config import ConfigurationError
from ...core.exceptions import log_exception
from ...database.core.session import Session
from ...database.core.orm import Table, Level, Topic, Host, Message, Subscriber, Access

# external libs
from cmdkit.app import Application, exit_status
from cmdkit.cli import Interface
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.attributes import InstrumentedAttribute


PROGRAM = 'streamkit database dump'
USAGE = f"""\
usage: {PROGRAM} [-h] TABLE [--join] [--head [N] | --tail [N]] [--csv [SEP]] [-o PATH]
{__doc__}\
"""

HELP = f"""\
{USAGE}

arguments:
TABLE                  Name of table to dump.

options:
    --join             Dump message table with full join.
    --head    [N]      Show first 'N' messages (default: 4).
    --tail    [N]      Show last 'N' messages (default: 4).
    --csv     [SEP]    Format output as CSV.
-o, --output  PATH     Path to output file (default: <stdout>).
-h, --help             Show this message and exit.\
"""


# initialize module level logger
log = logging.getLogger(__name__)


TABLES: Dict[str, Table] = {
    'level': Level,
    'topic': Topic,
    'host': Host,
    'message': Message,
    'subscriber': Subscriber,
    'access': Access
}


JOINS: Dict[str, Tuple[InstrumentedAttribute, ...]] = {
    'level': (),
    'topic': (),
    'host': (),
    'message': (Message.level, Message.topic, Message.host),
    'subscriber': (),
    'access': (Access.subscriber, Access.topic)
}

KEYS: Dict[str, InstrumentedAttribute] = {
    'level': Level.id,
    'topic': Topic.id,
    'host': Host.id,
    'message': Message.time,
    'subscriber': Subscriber.id,
    'access': Access.subscriber_id
}

# an iterable query or manifested list of rows
QueryResult = Union[Query, List[Table]]


class DumpDatabaseApp(Application):
    """Application class for database check entry-point."""

    interface = Interface(PROGRAM, USAGE, HELP)

    table: str = None
    interface.add_argument('table', choices=list(TABLES))

    join_all: bool = False
    interface.add_argument('--join', action='store_true', dest='join_all')

    head_count: Optional[int] = None
    tail_count: Optional[int] = None
    slice_interface = interface.add_mutually_exclusive_group()
    slice_interface.add_argument('--head', nargs='?', type=int, const=4, default=None, dest='head_count')
    slice_interface.add_argument('--tail', nargs='?', type=int, const=4, default=None, dest='tail_count')

    csv_format: Optional[str] = None
    interface.add_argument('--csv', nargs='?', const=',', default=None, dest='csv_format')

    outfile: IO = sys.stdout
    interface.add_argument('-o', '--output', dest='outfile', default=outfile)

    exceptions = {
        FileNotFoundError: functools.partial(log_exception, log=log.critical,
                                             status=exit_status.bad_argument),
        PermissionError: functools.partial(log_exception, log=log.critical,
                                           status=exit_status.runtime_error),
        RuntimeError: functools.partial(log_exception, log=log.critical,
                                        status=exit_status.runtime_error),
        ConfigurationError: functools.partial(log_exception, log=log.critical,
                                              status=exit_status.bad_config),
    }

    session: Session = None

    def run(self) -> None:
        """Business logic of command."""

        self.session = Session()
        results = self.query(self.table)
        if self.tail_count:
            results = reversed(results[:])

        if self.csv_format:
            names = TABLES[self.table].keys(joined=self.join_all)
            header = self.csv_format.join(list(names))
            print(header, file=self.outfile)

        for row in results:
            out = self.format_row(row.values(joined=self.join_all))
            print(out, file=self.outfile)

    @staticmethod
    def _format(row: Tuple, sep: str = ' ') -> str:
        return sep.join([str(value) for value in row])

    @staticmethod
    def _format_message(row: Tuple, sep: str = ',') -> str:
        return sep.join([str(value) for value in row[:-1]] + [f'"{row[-1]}"'])

    @functools.cached_property
    def format_row(self) -> Callable[[Tuple], str]:
        if self.csv_format is not None:
            bind = self._format if self.table != 'message' else self._format_message
            return functools.partial(bind, sep=self.csv_format)
        else:
            return self._format

    def query(self, table: str) -> Query:
        """Construct SQL query."""
        query = self.session.query(TABLES[table])
        if self.join_all:
            for relation in JOINS[table]:
                query = query.options(joinedload(relation))
        if self.head_count:
            return query.limit(self.head_count)
        if self.tail_count:
            return query.order_by(KEYS[table].desc()).limit(self.tail_count)
        else:
            return query

    def __enter__(self) -> DumpDatabaseApp:
        """Initialize output file."""
        if isinstance(self.outfile, str):
            self.outfile = open(self.outfile, mode='w')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.outfile is not sys.stdout:
            self.outfile.close()
