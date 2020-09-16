# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Database scoped session binding for StreamKit."""

# external libs
from sqlalchemy.orm import sessionmaker, scoped_session

# internal libs
from .engine import engine


factory = sessionmaker(bind=engine)
Session = scoped_session(factory)
