# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:54
@Author: Sue Zhu
"""

import sqlalchemy as sa

from .utils import BaseORM


class TradeCalendar(BaseORM):
    """ 交易日历 SSE上交所 """
    __tablename__ = 'trade_calendar'

    trade_dt = sa.Column(sa.Date, primary_key=True)
    is_d = sa.Column(sa.Integer)
    is_w = sa.Column(sa.Integer)
    is_m = sa.Column(sa.Integer)
    is_q = sa.Column(sa.Integer)
    is_y = sa.Column(sa.Integer)


