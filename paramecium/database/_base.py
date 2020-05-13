# -*- coding: utf-8 -*-
"""
@Time: 2020/2/22 11:47
@Author: Sue Zhu
"""
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

CUR_TS = sa.text('current_timestamp')

BaseORM = declarative_base(
    cls=type('_DabaseMeta', (object,), {
        'updated_at': sa.Column(sa.TIMESTAMP, server_default=CUR_TS, server_onupdate=CUR_TS)
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
