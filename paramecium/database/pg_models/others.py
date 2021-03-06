# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:54
@Author: Sue Zhu
"""
import sqlalchemy as sa

from .._postgres import *


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

    industry_code = sa.Column(type_=sa.String(40), primary_key=True)
    industry_name = sa.Column(type_=sa.String(40))
    level_num = sa.Column(sa.Integer, index=True)
    memo = sa.Column(sa.String)
    used = sa.Column(sa.Integer, index=True)


class InterestRate(BaseORM):
    """ 基准利率调整 """
    __tablename__ = 'macro_interest_rate'

    change_dt = sa.Column(sa.Date, primary_key=True)
    save_rate = sa.Column(sa.Float)  # 单位：%
    loan_rate = sa.Column(sa.Float)  # 单位：%
