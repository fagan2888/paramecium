"""Settings to override default settings."""

import logging

#
# Override settings
#
DEBUG = True
HTTP_PORT = 7777
HTTP_ADDRESS = '127.0.0.1'

#
# Set logging level
#
logging.getLogger().setLevel(logging.DEBUG)

JOB_CLASS_PACKAGES = ['simple_scheduler.jobs']

# Postgres
#
DATABASE_CLASS = 'ndscheduler.corescheduler.datastore.providers.postgres.DatastorePostgres'
DATABASE_CONFIG_DICT = {
    'user': 'postgres',
    'password': 'qwe123',
    'hostname': '127.0.0.1',
    'port': 5432,
    'database': 'postgres',
    'sslmode': 'disable'
}