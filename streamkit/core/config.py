# This program is free software: you can redistribute it and/or modify it under the
# terms of the Apache License (v2.0) as published by the Apache Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the Apache License for more details.
#
# You should have received a copy of the Apache License along with this program.
# If not, see <https://www.apache.org/licenses/LICENSE-2.0>.

"""Runtime configuration for StreamKit."""


# standard libs
import os
import sys
import subprocess
import functools

# external libs
import toml
from cmdkit.config import Namespace, Configuration
from cmdkit.app import exit_status

# internal libs
from .logging import Logger, HANDLERS_BY_NAME, LEVELS_BY_NAME, LEVELS
from ..assets import load_asset


# module level logger
log = Logger(__name__)


# environment variables and configuration files are automatically
# depth-first merged with defaults
DEFAULT = Namespace({
    'database': {
            'backend': 'sqlite',
            'database': ':memory:'
    },
    'logging': {
        'level': 'warning',
        'handler': 'standard',
    }
})


CWD = os.getcwd()
HOME = os.getenv('HOME')
if os.name == 'nt':
    ROOT = False
    SITE = 'user'
    USER_SITE = os.path.join(os.getenv('APPDATA'), 'streamkit')
else:
    ROOT = os.getuid() == 0
    SITE = 'system' if ROOT else 'user'
    USER_SITE = os.path.join(HOME, '.streamkit')

CONF_PATH = {
    'system': '/etc/streamkit.toml',  # only for POSIX
    'user': os.path.join(USER_SITE, 'config.toml'),
    'local': os.path.join(CWD, '.streamkit', 'config.toml')
}


def init_config(key: str = None) -> None:
    """Initialize configuration with defaults if necessary."""
    site = SITE if key is None else key
    path = CONF_PATH[site]
    if not os.path.exists(path):
        default = toml.loads(load_asset('config/streamkit.toml'))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode='w') as config_file:
            toml.dump(default, config_file)


@functools.lru_cache(maxsize=1)
def get_site(key: str = None) -> str:
    """
    Return the runtime site.
    Automatically creates directories if needed.
    """
    path = CONF_PATH[SITE] if key is None else CONF_PATH[key]
    return path


def get_config() -> Configuration:
    """Load configuration."""
    return Configuration.from_local(env=True, prefix='STREAMKIT',
                                    default=DEFAULT, system=CONF_PATH['system'],
                                    user=CONF_PATH['user'], local=CONF_PATH['local'])


# global instance, some uses may reload this
config: Configuration = get_config()


class ConfigurationError(Exception):
    """Exception specific to configuration errors."""


def expand_parameters(prefix: str, namespace: Namespace) -> str:
    """Substitute values into namespace if `_env` or `_eval` present."""
    value = None
    count = 0
    for key in filter(lambda _key: _key.startswith(prefix), namespace.keys()):
        count += 1
        if count > 1:
            raise ValueError(f'more than one variant of "{prefix}" in configuration file')
        if key.endswith('_env'):
            value = os.getenv(namespace[key])
            log.debug(f'expanded "{prefix}" from configuration as environment variable')
        elif key.endswith('_eval'):
            value = subprocess.check_output(namespace[key].split()).decode().strip()
            log.debug(f'expanded "{prefix}" from configuration as shell command')
        elif key == prefix:
            value = namespace[key]
        else:
            raise ValueError(f'unrecognized variant of "{prefix}" ({key}) in configuration file')
    return value


def update_config(site: str, data: dict) -> None:
    """
    Extend the current configuration and commit it to disk.

    Parameters:
        site (str):
            Either "local", "user", or "system"
        data (dict):
            Sectioned mappable to update configuration file.

    Example:
        >>> update_config('user', {
        ...    'database': {
        ...        'user': 'ABC123'
        ...    }
        ... })
    """
    init_config(site)  # ensure default exists
    new_config = Configuration(old=get_config().namespaces[site],
                               new=Namespace(data))
    # commit to file
    new_config._master.to_local(CONF_PATH[site])  # noqa: accessing protected member


# setup logging with new info from configuration
LOGGING_LEVEL = config['logging']['level']
try:
    LOGGING_LEVEL = LEVELS_BY_NAME[str(LOGGING_LEVEL).upper()]
except KeyError:
    try:
        LOGGING_LEVEL = LEVELS[int(LOGGING_LEVEL)]
    except (ValueError, IndexError):
        log.critical(f'unknown level: {LOGGING_LEVEL}')
        sys.exit(exit_status.bad_config)


LOGGING_HANDLER = config['logging']['handler'].upper()
try:
    LOGGING_HANDLER = HANDLERS_BY_NAME[LOGGING_HANDLER]
except KeyError:
    log.critical(f'unknown handler: {LOGGING_HANDLER}')
    sys.exit(exit_status.bad_config)


# set initial handler by environment variable or default
LOGGING_HANDLER.level = LOGGING_LEVEL
log.handlers[0] = LOGGING_HANDLER
