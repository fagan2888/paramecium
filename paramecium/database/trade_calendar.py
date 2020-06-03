# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:54
@Author: Sue Zhu
"""
__all__ = ['get_dates', 'last_td_date']

from functools import lru_cache

import pandas as pd
import sqlalchemy as sa

from paramecium.const import FreqEnum
from paramecium.database.utils import BaseORM, get_session, flat_1dim


class TradeCalendar(BaseORM):
    """ 交易日历 SSE上交所 """
    __tablename__ = 'trade_calendar'

    trade_dt = sa.Column(sa.Date, primary_key=True)
    is_d = sa.Column(sa.Integer)
    is_w = sa.Column(sa.Integer)
    is_m = sa.Column(sa.Integer)
    is_q = sa.Column(sa.Integer)
    is_y = sa.Column(sa.Integer)


@lru_cache()
def get_dates(freq=None):
    with get_session() as session:
        query = session.query(TradeCalendar.trade_dt)
        if freq:
            if isinstance(freq, FreqEnum):
                freq = freq.name
            query = query.filter(getattr(TradeCalendar, f'is_{freq.lower()}') == 1)
        data = flat_1dim(query.all())
    return pd.to_datetime(sorted(data))


def last_td_date():
    cur_date = pd.Timestamp.now()
    if cur_date.hour <= 22:
        cur_date -= pd.Timedelta(days=1)
    return max((t for t in get_dates(freq=FreqEnum.D) if t <= cur_date))
