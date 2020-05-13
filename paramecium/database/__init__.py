# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 17:07
@Author: Sue Zhu
"""
from ._base import BaseORM, TradeCalendar
from ._local_ts import get_engine, ts_api


def create_all_table():
    from . import model_stock_org, model_fund_org, model_const
    BaseORM.metadata.create_all(get_engine())
