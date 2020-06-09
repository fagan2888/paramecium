# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:54
@Author: Sue Zhu
"""
from .utils import *


class TradeCalendar(BaseORM):
    """ 交易日历 SSE上交所 """
    __tablename__ = 'trade_calendar'

    trade_dt = sa.Column(sa.Date, primary_key=True)
    is_d = sa.Column(sa.Integer)
    is_w = sa.Column(sa.Integer)
    is_m = sa.Column(sa.Integer)
    is_q = sa.Column(sa.Integer)
    is_y = sa.Column(sa.Integer)


class EnumIndustryCode(BaseORM):
    __tablename__ = 'enum_industry_code'

    code = sa.Column(name='industry_code', type_=sa.String(40), primary_key=True)
    name = sa.Column(name='industry_name', type_=sa.String(40))
    level_num = sa.Column(sa.Integer)
    memo = sa.Column(sa.String)


class InterestRate(BaseORM):
    """ 基准利率调整 """
    __tablename__ = 'macro_interest_rate'

    change_dt = sa.Column(sa.Date, primary_key=True)
    save_rate = sa.Column(pg.REAL)  # 单位：%
    loan_rate = sa.Column(pg.REAL)  # 单位：%