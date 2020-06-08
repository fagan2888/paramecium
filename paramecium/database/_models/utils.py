# -*- coding: utf-8 -*-
"""
@Time: 2020/2/22 11:47
@Author: Sue Zhu
"""
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy.ext.declarative import declarative_base

gen_oid = lambda: sa.Column(pg.UUID, server_default=sa.text('uuid_generate_v4()'), primary_key=True)

BaseORM = declarative_base(
    cls=type('DBMeta', (object,), {
        'updated_at': sa.Column(
            sa.TIMESTAMP,
            server_default=sa.func.current_timestamp(),
            server_onupdate=sa.func.current_timestamp()
        ),
        'get_primary_key': classmethod(lambda cls: cls._sa_class_manager.mapper.primary_key)
    })
)
