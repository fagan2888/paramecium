"""Settings to override default settings."""

import logging

# Override settings
from paramecium.tools.data_api import get_data_config

DEBUG = True
HTTP_PORT = 7777
HTTP_ADDRESS = '127.0.0.1'

# Set logging level
logging.getLogger().setLevel(logging.DEBUG)

JOB_CLASS_PACKAGES = ['simple_scheduler.jobs']

# Postgres
_db = get_data_config('postgres')
DATABASE_CLASS = 'ndscheduler.corescheduler.datastore.providers.postgres.DatastorePostgres'
DATABASE_CONFIG_DICT = {
    'user': _db['username'],
    'password': _db['password'],
    'hostname': _db['host'],
    'port': int(_db['port']),
    'database': _db['database'],
    'sslmode': 'disable'
}