# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 17:07
@Author: Sue Zhu
"""
__all__ = [
    'get_dates', 'last_td_date',
    'get_session', 'get_sql_engine',
    'StockUniverse', 'FundUniverse',
    'get_price', 'get_sector',
]

from functools import lru_cache

import numpy as np
import pandas as pd
import sqlalchemy as sa

from . import fund_org, stock_org, enum_code, index_org
from .trade_calendar import get_dates, last_td_date
from .utils import get_session, get_sql_engine, BaseORM, flat_1dim, logger
from ..const import *
from ..interface import AbstractUniverse


def create_all_table():
    from .utils import BaseORM
    logger.info('creating all sqlalchemy data models')
    BaseORM.metadata.create_all(get_sql_engine('postgres'))


# ------------- Universe -----------------------------------------------------------
@lru_cache()
class StockUniverse(AbstractUniverse):

    def __init__(self, issue_month=12, no_st=True, no_suspend=True):
        self.issue = issue_month * 30
        self.no_st = no_st
        self.no_suspend = no_suspend

    @lru_cache(maxsize=2)
    def get_instruments(self, dt):
        with get_session() as session:
            query_desc = session.query(
                stock_org.AShareDescription.wind_code,
            ).filter(
                stock_org.AShareDescription.list_dt <= dt - pd.Timedelta(days=self.issue),
                stock_org.AShareDescription.delist_dt >= dt,
            ).all()
            query_trade = session.query(
                stock_org.AShareEODPrice.wind_code
            ).filter(
                stock_org.AShareEODPrice.trade_dt == dt,
                stock_org.AShareEODPrice.trade_status == 0
            ).all()
            # TODO: need data to clean st stock

        return {*flat_1dim(query_desc)} - {*flat_1dim(query_trade)}


@lru_cache()
class FundUniverse(AbstractUniverse):

    def __init__(self, include_=None, exclude_=None, initial_only=True, no_grad=True,
                 open_only=True, issue_month=12, size_=0.5):
        self.include = include_
        self.exclude = exclude_
        self.initial_only = initial_only
        self.no_grad = no_grad
        self.open_only = open_only
        self.issue = issue_month * 30  # simply think there is 30 days each month.
        self.size = size_

    @lru_cache(maxsize=2)
    def get_instruments(self, dt):
        # filters = [model_fund_org.MutualFundDescription.setup_date <= dt - pd.Timedelta(days=self.issue)]
        # if self.include:
        #     filters.append(model_fund_org.MutualFundDescription.invest_type.in_(self.include))
        # if self.exclude:
        #     filters.append(model_fund_org.MutualFundDescription.invest_type.notin_(self.exclude))
        #
        # if self.no_grad:
        #     filters.append(model_fund_org.MutualFundDescription.grad_type < 1)
        # if self.open_only:
        #     filters.append(model_fund_org.MutualFundDescription.fund_type == '契约型开放式')
        #
        # TODO: Size filter has not apply
        pass


# ------------- Price -----------------------------------------------------------
def get_price(asset: AssetEnum, start=None, end=None, code=None, fields=None):
    if asset == AssetEnum.STOCK:
        model = stock_org.AShareEODPrice
    elif asset == AssetEnum.CMF:
        model = fund_org.MutualFundNav
    else:
        raise KeyError(f'Undefined Asset {asset.value}')

    filters = []
    if start:
        filters.append(model.trade_dt >= start)
    if end:
        filters.append(model.trade_dt >= end)
    if code:
        filters.append(model.wind_code == code)

    with get_session() as session:
        data = pd.DataFrame(session.query(model).filter(*filters)).fillna(np.nan)

    data.loc[:, 'trade_dt'] = pd.to_datetime(data['trade_dt'])

    if fields:
        data = data.filter(fields, axis=1)
    else:
        data = data.drop(['oid', 'updated_at'], axis=1, errors='ignore')

    return data


def get_sector(asset: AssetEnum, valid_dt=None, sector_type=None):
    table = BaseORM.metadata.tables[f'{asset.value}_org_sector']

    filters = []
    if valid_dt:
        filters.extend((valid_dt >= table.c.entry_dt, valid_dt <= table.c.remove_dt))
    if sector_type:
        start_code = sector_type.value
        filters.append(
            sa.func.substr(table.c.sector_code, 1, len(start_code)) == start_code
        )
    with get_session() as session:
        data = pd.DataFrame(session.query(table).filter(*filters).all()).fillna(np.nan)
        for t_col in ('entry_dt', 'remove_dt'):
            data.loc[:, t_col] = pd.to_datetime(data[t_col])
        data.drop(['oid', 'updated_at'], axis=1, errors='ignore')

    return data
