# -*- coding: utf-8 -*-
"""
@Time: 2020/6/8 9:54
@Author: Sue Zhu
"""
__all__ = [
    'BaseORM', 'gen_oid', 'gen_update',
    'get_sql_engine', 'get_session', 'try_commit',
    'get_or_create_table', 'create_all_table', 'upsert_data', 'bulk_insert', 'clean_duplicates'
]

import logging
from contextlib import contextmanager

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as sa_err
from sqlalchemy import orm as sa_orm
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

from ..configuration import get_data_config

logger = logging.getLogger(__name__)


def _df2list(raw_data):
    if isinstance(raw_data, pd.DataFrame):
        return [record.dropna().to_dict() for _, record in raw_data.iterrows()]
    else:
        return raw_data


def try_commit(session, on_doing, success=''):
    try:
        session.commit()
        if success:
            logger.info(success)
    except sa_err.DataError as e:
        logger.error(f'step into breakpoint for {repr(e)}')
        breakpoint()
    except Exception as e:
        logger.error(f'fail to commit session when {on_doing} with {e!r}')
        session.rollback()


def gen_oid():
    return sa.Column('oid', pg.UUID, server_default=sa.text('uuid_generate_v4()'), primary_key=True)


def gen_update():
    return sa.Column(
        'updated_at', sa.TIMESTAMP,
        server_default=sa.func.current_timestamp(),
        server_onupdate=sa.func.current_timestamp()
    )


BaseORM = declarative_base(
    cls=type('DBMeta', (object,), {
        'updated_at': gen_update(),
        'get_primary_key': classmethod(lambda cls: cls._sa_class_manager.mapper.primary_key)
    })
)


def get_sql_engine(**kwargs):
    params = dict(pool_size=30, encoding='utf-8')
    params.update(**kwargs)
    return sa.create_engine(URL(**get_data_config('postgres')), **params)


_Session = sa_orm.scoped_session(sa_orm.sessionmaker(bind=get_sql_engine()))  # echo=True


@contextmanager
def get_session():
    session = _Session()
    yield session
    session.close()


def create_all_table():
    # from .pg_models import stock, fund, index, monitors, others
    logger.info('creating all sqlalchemy data models')
    BaseORM.metadata.create_all(get_sql_engine())

    # create some view for query.
    with get_session() as session:
        try_commit(session, 'create instruments view')
        session.execute(
            f"""
            create or replace view index_price as
            select wind_code, trade_dt, close_
            from index_org_price p1
            union all 
            select benchmark_code, trade_dt, close_
            from index_derivative_price
            """
        )
        try_commit(session, 'create index price view')


def get_or_create_table(name, *columns, **kwargs):
    engine = get_sql_engine()  # echo=True
    BaseORM.metadata.reflect(bind=engine, views=True)
    if name not in BaseORM.metadata.tables:
        if columns:
            table = sa.Table(
                name, BaseORM.metadata,
                gen_oid(), gen_update(), *columns,
                keep_existing=True, **kwargs
            )
            table.create(bind=engine)
        else:
            raise sa_err.NoSuchTableError("Table")

    return BaseORM.metadata.tables[name]


def bulk_insert(records, model):
    with get_session() as session:
        if isinstance(model, sa.Table):
            session.execute(pg.insert(model, _df2list(records)))
            try_commit(session, f'normal insert data for {model.key}')
        else:
            session.bulk_insert_mappings(model, _df2list(records))
            try_commit(session, f'bulk insert data for {model.__tablename__}')


def upsert_data(records, model, ukeys=None):
    result_ids = []
    with get_session() as session:
        for record in _df2list(records):
            insert_exe = pg.insert(model).values(**record)
            if ukeys:
                set_ = {k: sa.text(f'EXCLUDED.{k}') for k in {*record.keys()} - {*(c.key for c in ukeys)}}
                insert_exe = insert_exe.on_conflict_do_update(
                    index_elements=ukeys,
                    set_={**set_, 'updated_at': sa.func.current_timestamp()}
                )
            exe_result = session.execute(insert_exe)
            result_ids.extend(exe_result.inserted_primary_key)

        try_commit(session, f'upsert data for {model.__tablename__}')

    return result_ids


def clean_duplicates(model, unique_cols):
    with get_session() as session:
        labeled_cols = [c.label(c.key) for c in unique_cols]
        pk, *_ = model.get_primary_key()
        grouped = sa_orm.Query(labeled_cols).group_by(*unique_cols).having(
            sa.func.count(pk) > 1
        ).subquery('g')
        duplicates = pd.DataFrame(
            session.query(pk, *labeled_cols).join(
                grouped, sa.and_(*(grouped.c[c.key] == c for c in unique_cols))
            ).order_by(model.updated_at).all()
        )
        if not duplicates.empty:
            session.query(model).filter(pk.in_(
                duplicates.set_index(pk.name).loc[lambda df: df.duplicated(keep='last')].index.tolist()
            )).delete(synchronize_session='fetch')

        try_commit(session, f'clean duplicates for {model.__tablename__}')
