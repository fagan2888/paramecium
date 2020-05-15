# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 17:07
@Author: Sue Zhu
"""
from ..tools.db_source import get_tushare_api
from ._base import BaseORM, TradeCalendar


def create_all_table():
    from . import model_stock_org, model_fund_org, model_const
    BaseORM.metadata.create_all(get_tushare_api())
