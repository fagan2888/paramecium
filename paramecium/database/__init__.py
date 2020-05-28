# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 17:07
@Author: Sue Zhu
"""
__all__ = [
    'create_all_table', 'get_session', 'get_sql_engine',
    'get_dates','last_td_date',
    'StockUniverse', 'FundUniverse',
]

from functools import lru_cache

import pandas as pd

from paramecium.const import *
from paramecium.database import model_fund_org, model_stock_org, model_const, get_session
from paramecium.database.model_market import TradeCalendar
from paramecium.database.utils import create_all_table, get_session, get_sql_engine
from paramecium.interface import AbstractUniverse


def _flat_1dim(folder_data):
    return (entry for record in folder_data for entry in record)


# calendar
@lru_cache()
def get_dates(freq=None):
    with get_session() as session:
        query = session.query(TradeCalendar.trade_dt)
        if freq:
            if isinstance(freq, FreqEnum):
                freq = freq.name
            query = query.filter(getattr(TradeCalendar, f'is_{freq.lower()}'))
        data = _flat_1dim(query.all())
    return pd.to_datetime(list(data))


def last_td_date():
    cur_date = pd.Timestamp.now()
    if cur_date.hour <= 22:
        cur_date -= pd.Timedelta(days=1)
    return max((t for t in get_dates(freq=FreqEnum.D) if t <= cur_date))


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
                model_stock_org.AShareDescription.wind_code,
            ).filter(
                model_stock_org.AShareDescription.list_dt <= dt - pd.Timedelta(days=self.issue),
                model_stock_org.AShareDescription.delist_dt >= dt,
            ).all()
            query_trade = session.query(
                model_stock_org.AShareEODPrice.wind_code
            ).filter(
                model_stock_org.AShareEODPrice.trade_dt == dt,
                model_stock_org.AShareEODPrice.trade_status == 0
            ).all()
            # TODO: need data to clean st stock

        return {*_flat_1dim(query_desc)} - {*_flat_1dim(query_trade)}


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


def get_price(asset:AssetEnum, start=None, end=None, codes=None, fields=None):
    if asset == AssetEnum.STOCK:
        with get_session() as session:
            price_df = pd.DataFrame(

            )

