# -*- coding: utf-8 -*-
"""
@Time: 2020/2/22 11:47
@Author: Sue Zhu
"""
from contextlib import contextmanager

import sqlalchemy as sa

from paramecium.utils.configuration import get_data_config

BaseORM = sa.ext.declarative.declarative_base(
    cls=type('_DabaseMeta', (object,), {
        'updated_at': sa.Column(
            sa.TIMESTAMP,
            server_default=sa.func.current_timestamp(),
            server_onupdate=sa.func.current_timestamp()
        ),
        'get_primary_key': classmethod(lambda cls: cls._sa_class_manager.mapper.primary_key)
    })
)


def get_sql_engine(env='postgres', **kwargs):
    params = dict(pool_size=30, encoding='utf-8')
    params.update(**kwargs)
    return sa.create_engine(sa.engine.URL(**get_data_config(env)), **params)


_Sa_Session = sa.orm.scoped_session(sa.orm.sessionmaker(bind=get_sql_engine(env='postgres')))


def create_all_table():
    BaseORM.metadata.create_all(get_sql_engine('postgres'))


@contextmanager
def get_session():
    session = _Sa_Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
    finally:
        session.close()
