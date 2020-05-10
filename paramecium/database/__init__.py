# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 17:07
@Author:  MUYUE1
"""
from ._local_ts import get_engine, ts_api
from ._base import BaseORM


def create_all_table():
    from . import model_stock_org
    BaseORM.metadata.create_all(get_engine())
