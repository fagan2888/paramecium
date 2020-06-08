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

from ..configuration import get_data_config
from ._models.utils import BaseORM

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
    try:
        yield session
        session.commit()
    except DataError as e:
        logger.error(f'step into breakpoint for {repr(e)}')
        breakpoint()
    except Exception as e:
        logger.error(f'fail to commit session for {e!r}')
        session.rollback()
    finally:
        session.close()


def create_all_table():
    from ._models import others, fund, stock_org, index, macro
    logger.info('creating all sqlalchemy data models')
    BaseORM.metadata.create_all(get_sql_engine('postgres'))

    # create some view for query.
    with get_session() as ss:
        ss.execute(
            f"""
            create or replace view index_price as
            select wind_code, trade_dt, close_
            from index_org_price p1
            union all 
            select benchmark_code, trade_dt, close_
            from index_derivative_price
            """
        )


def get_table_by_name(name):
    return BaseORM.metadata.tables[name]


def bulk_insert(records, model):
    with get_session() as session:
        session.bulk_insert_mappings(model, records)


def upsert_data(records, model, ukeys=None):
    result_ids = []
    with get_session() as session:
        for record in records:
            insert_exe = insert(model).values(**record)
            if ukeys:
                set_ = {k: sa.text(f'EXCLUDED.{k}') for k in {*record.keys()} - {*(c.key for c in ukeys)}}
                insert_exe = insert_exe.on_conflict_do_update(
                    index_elements=ukeys, set_={**set_, 'updated_at': sa.func.current_timestamp()}
                )
            exe_result = session.execute(insert_exe)
            result_ids.extend(exe_result.inserted_primary_key)

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
