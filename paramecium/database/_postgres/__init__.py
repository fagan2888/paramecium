# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 9:41
@Author: Sue Zhu
"""
import logging
from contextlib import contextmanager
from functools import lru_cache

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import orm as sa_orm
from sqlalchemy.engine.url import URL as sa_url
from sqlalchemy.exc import DataError

from . import cal_
from .utils import BaseORM
from .._tool import flat_1dim
from ...configuration import get_data_config
from ...const import FreqEnum
from ...interface import AbstractFactorIO

logger = logging.getLogger(__name__)


def get_sql_engine(env='_postgres', **kwargs):
    params = dict(pool_size=30, encoding='utf-8')
    params.update(**kwargs)
    return sa.create_engine(sa_url(**get_data_config(env)), **params)


_Session = sa_orm.scoped_session(sa_orm.sessionmaker(
    bind=get_sql_engine(env='_postgres')  # , echo=True
))


@contextmanager
def get_session():
    session = _Session()
    logger.debug(f'sa session create')
    try:
        yield session
        session.commit()
    except DataError as e:
        print(f'step into breakpoint for {repr(e)}')
        breakpoint()
    except Exception as e:
        logger.error(f'fail to commit session for {e!r}')
        session.rollback()
    finally:
        logger.debug(f'sa session closed')
        session.close()


def bulk_insert(records, model):
    with get_session() as session:
        logger.info(f'bulk insert {len(records)} data into {model}...')
        session.bulk_insert_mappings(model, records)


def create_all_table():
    from . import fund_org, index_org, macro, stock_org, cal_
    logger.info('creating all sqlalchemy data models')
    BaseORM.metadata.create_all(get_sql_engine('_postgres'))
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


@lru_cache()
def get_dates(freq=None):
    with get_session() as session:
        query = session.query(cal_.TradeCalendar.trade_dt)
        if freq:
            if isinstance(freq, FreqEnum):
                freq = freq.name
            query = query.filter(getattr(cal_.TradeCalendar, f'is_{freq.lower()}') == 1)
        data = flat_1dim(query.all())
    return pd.to_datetime(sorted(data))


def get_last_td_date():
    cur_date = pd.Timestamp.now()
    if cur_date.hour <= 22:
        cur_date -= pd.Timedelta(days=1)
    return max((t for t in get_dates(freq=FreqEnum.D) if t <= cur_date))


class EnumIndustryCode(BaseORM):
    __tablename__ = 'enum_industry_code'

    code = sa.Column(name='industry_code', type_=sa.String(40), primary_key=True)
    name = sa.Column(name='industry_name', type_=sa.String(40))
    level_num = sa.Column(sa.Integer)
    memo = sa.Column(sa.String)


class FactorDBTool(AbstractFactorIO):

    def __init__(self, factor: 'AbstractFactor'):
        super().__init__(factor)
        with get_session() as session:
            if not session.bind.has_table(self.table_name):
                self.logger.info('Factor Table do not exist, create it first.')
                self._factor.get_empty_table().to_sql(**self.saving_param, index=False)
                session.execute(
                    f"""
                    alter table {self.table_name} add oid uuid default uuid_generate_v4();
                    alter table {self.table_name} add updated_at timestamp default current_timestamp;
                    alter table {self.table_name} add constraint pk_{self.table_name} primary key(oid);
                    
                    create index ix_{self.table_name}_code on {self.table_name} (wind_code);
                    create index ix_{self.table_name}_dt on {self.table_name} (trade_dt);
                    """
                )

    @property
    def saving_param(self):
        return dict(
            name=self.table_name, con=get_sql_engine('_postgres'),
            dtype={'wind_code': sa.String(40), 'trade_dt': sa.Date, **self._factor.field_types}
        )

    def localized_snapshot(self, dt, if_exist=1):
        """
        计算并保存单日因子
        :param dt: pd.Timestamp
        :param if_exist: {1: replace, 0: error}, 由于因子在截面上有相关性，不考虑update部分的情况
        :return:
        """
        # check if data exist
        if if_exist == 1:
            with get_session() as session:
                session.execute(f"delete from {self.table_name} where trade_dt='{dt:%Y-%m-%d}'")
        else:
            exist_rows = pd.read_sql(
                f"select count(*) from {self.table_name} where trade_dt='{dt:%Y-%m-%d}'",
                get_sql_engine('_postgres'),
            ).squeeze()
            if exist_rows:
                msg = f"factor data at {dt:%Y-%m-%d} is exist, please turn into replace mode or check your code!"
                self.logger.error(msg)
                raise KeyError(msg)

        self.logger.debug(f'compute factor at {dt:%Y-%m-%d}')
        snapshot = self._factor.compute(dt)
        if snapshot.empty:
            self.logger.warning(f'empty data at {dt:%Y-%m-%d} need to be check!')
        else:
            snapshot.index.name = 'wind_code'
            snapshot.assign(trade_dt=dt).to_sql(**self.saving_param, method='multi', if_exists='append')

    def fetch_snapshot(self, dt):
        snapshot = pd.read_sql(
            f"select * from {self.table_name} where trade_dt='{dt:%Y-%m-%d}'",
            con=get_sql_engine('_postgres'), index_col=['wind_code'],
        ).drop(['updated_at', 'oid', 'trade_dt'], errors='ignore', axis=1)
        if snapshot.empty:
            snapshot = self.localized_snapshot(dt, if_exist=1)
        return snapshot

    def _get_dates(self, start, end, freq):
        if start is None:
            real_start = self._factor.start_date
        else:
            real_start = max((t for t in get_dates(freq) if self._factor.start_date <= t <= pd.Timestamp(start)))
        real_end = min((get_last_td_date(), pd.Timestamp(end) if end is not None else pd.Timestamp.max))
        return (t for t in get_dates(freq) if real_start <= t <= real_end)

    def get_latest_date(self):
        return pd.to_datetime(pd.read_sql(
            f"select max(trade_dt) from {self.table_name}", get_sql_engine('_postgres'),
        ).squeeze())
