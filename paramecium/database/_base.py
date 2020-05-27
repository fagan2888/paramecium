# -*- coding: utf-8 -*-
"""
@Time: 2020/2/22 11:47
@Author: Sue Zhu
"""
from functools import partial

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from ..tools.data_api import get_sql_engine

CUR_TS = sa.func.current_timestamp()

BaseORM = declarative_base(
    cls=type('_DabaseMeta', (object,), {
        'updated_at': sa.Column(sa.TIMESTAMP, server_default=CUR_TS, server_onupdate=CUR_TS),
        'get_primary_key': classmethod(lambda cls: cls._sa_class_manager.mapper.primary_key)
    })
)


class TradeCalendar(BaseORM):
    """ 交易日历 SSE上交所 """
    __tablename__ = 'trade_calendar'

    trade_dt = sa.Column(sa.Date, primary_key=True)
    is_d = sa.Column(sa.Integer)
    is_w = sa.Column(sa.Integer)
    is_m = sa.Column(sa.Integer)
    is_q = sa.Column(sa.Integer)
    is_y = sa.Column(sa.Integer)


def create_all_table():
    from . import model_stock_org, model_fund_org, model_const
    BaseORM.metadata.create_all(get_sql_engine('postgres'))
