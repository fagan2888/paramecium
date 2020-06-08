"""Settings to override default settings."""

import logging

from paramecium.configuration import get_data_config

# Override settings
DEBUG = True
HTTP_PORT = 7777
HTTP_ADDRESS = '127.0.0.1'
# DEBUG = False

# Set logging level
logging.getLogger().setLevel(logging.DEBUG)

JOB_CLASS_PACKAGES = ['scheduler_.jobs']

# Postgres
DATABASE_CLASS = 'ndscheduler.corescheduler.datastore.providers._postgres.DatastorePostgres'
_db = get_data_config('_postgres')
DATABASE_CONFIG_DICT = {
    'user': _db['username'],
    'password': _db['password'],
    'hostname': _db['host'],
    'port': int(_db['port']),
    'database': _db['database'],
    'sslmode': 'disable'
}