"""Settings to override default settings."""

import logging

from paramecium.configuration import get_data_config

# Override settings
DEBUG = False
HTTP_PORT = 7777
HTTP_ADDRESS = '127.0.0.1'

# Set logging level
logging.getLogger().setLevel(logging.DEBUG)

JOB_CLASS_PACKAGES = ['paramecium.database._crawler']
# Postgres
DATABASE_CLASS = 'ndscheduler.corescheduler.datastore.providers.postgres.DatastorePostgres'
_db = get_data_config('postgres')
DATABASE_CONFIG_DICT = {
    'user': _db['username'],
    'password': _db['password'],
    'hostname': _db['host'],
    'port': int(_db['port']),
    'database': _db['database'],
    'sslmode': 'disable'
}
