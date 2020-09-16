# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Database initialization for StreamKit."""


# type annotations
from __future__ import annotations
from typing import TypeVar

# standard libs
import json
from datetime import datetime

# internal libs
from .engine import engine, db_config, schema
from .session import Session
from .orm import Table, tables
from ...core.logging import Logger
from ... import assets


# module level logger
log = Logger(__name__)


def init() -> None:
    """Initialize database objects (e.g., tables)."""
    Table.metadata.create_all(engine)


__VT = TypeVar('__VT', int, float, str)
__RT = TypeVar('__RT', int, float, str, datetime)
def _coerce_datetime(field: str, value: __VT) -> __RT:
    """Passively coerce formatted datetime strings if necessary."""
    if isinstance(value, str) and field.endswith('created'):
        return datetime.strptime(value, '%Y-%m-%d')
    else:
        return value


def load_records(name: str) -> list:
    """Load records for given table from `name`."""
    data = json.loads(assets.load_asset(f'database/test/{name}.json'))
    table = tables[name]
    return [table(**{k: _coerce_datetime(k, v) for k, v in record.items()})
            for record in data]


def init_extensions() -> None:
    """Initialize database extensions/extras."""
    init()  # does nothing if all tables exist
    session = Session()
    backend = db_config['backend']
    scripts = assets.find_files(f'/database/extensions/{backend}/*.sql')
    for path in scripts:
        code = assets.load_asset(path).replace('{{ SCHEMA }}', schema or 'public')
        log.info(f'running {path}')
        session.execute(code)
        session.commit()


def init_test_data() -> None:
    """Initialize database objects and test data for unit tests."""
    init()  # does nothing if all tables exist
    session = Session()
    for name in tables:
        session.add_all(load_records(name))
        session.commit()
