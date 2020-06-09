# -*- coding: utf-8 -*-
"""
@Time: 2020/6/8 9:54
@Author: Sue Zhu
"""
import logging
from contextlib import contextmanager

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import orm as sa_orm
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.url import URL as sa_url
from sqlalchemy.exc import DataError

from ._models.utils import BaseORM, gen_update, gen_oid
from ..configuration import get_data_config

logger = logging.getLogger(__name__)


def get_sql_engine(env='postgres', **kwargs):
    params = dict(pool_size=30, encoding='utf-8')
    params.update(**kwargs)
    return sa.create_engine(sa_url(**get_data_config(env)), **params)


_Session = sa_orm.scoped_session(sa_orm.sessionmaker(
    bind=get_sql_engine(env='postgres')  # , echo=True
))


@contextmanager
def get_session():
    session = _Session()
    yield session
    session.close()


def create_all_table():
    logger.info('creating all sqlalchemy data models')
    BaseORM.metadata.create_all(get_sql_engine('postgres'))

    # create some view for query.
    with get_session() as session:
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
        try:
            session.commit()
        except DataError as e:
            logger.error(f'step into breakpoint for {repr(e)}')
            breakpoint()
        except Exception as e:
            logger.error(f'fail to commit session when create index price view {e!r}')
            session.rollback()


def get_table_by_name(name):
    return BaseORM.metadata.tables[name]


def bulk_insert(records, model):
    with get_session() as session:
        session.bulk_insert_mappings(model, records)

        try:
            session.commit()
        except DataError as e:
            logger.error(f'step into breakpoint for {repr(e)}')
            breakpoint()
        except Exception as e:
            logger.error(f'fail to commit session when bulk insert data for {model.__tablename__} {e!r}')
            session.rollback()


def upsert_data(records, model, ukeys=None):
    result_ids = []
    with get_session() as session:
        for record in records:
            insert_exe = insert(model).values(**record)
            if ukeys:
                set_ = {k: sa.text(f'EXCLUDED.{k}') for k in {*record.keys()} - {*(c.key for c in ukeys)}}
                insert_exe = insert_exe.on_conflict_do_update(
                    index_elements=ukeys,
                    set_={**set_, 'updated_at': sa.func.current_timestamp()}
                )
            exe_result = session.execute(insert_exe)
            result_ids.extend(exe_result.inserted_primary_key)

        try:
            session.commit()
        except DataError as e:
            logger.error(f'step into breakpoint for {repr(e)}')
            breakpoint()
        except Exception as e:
            logger.error(f'fail to commit session when upsert data for {model.__tablename__} {e!r}')
            session.rollback()

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

        try:
            session.commit()
        except DataError as e:
            logger.error(f'step into breakpoint for {repr(e)}')
            breakpoint()
        except Exception as e:
            logger.error(f'fail to commit session when clean duplicates for {model.__tablename__} {e!r}')
            session.rollback()


def get_or_create_table(name, *columns, **kwargs):
    table = sa.Table(
        name, BaseORM.metadata,
        gen_oid(), gen_update(), *columns,
        keep_existing=True, **kwargs
    )
    engine = get_sql_engine('postgres', echo=True)
    if not engine.has_table(name):
        table.create(bind=engine)
    return table
