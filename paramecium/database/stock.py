# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 14:41
@Author: Sue Zhu
"""
from functools import lru_cache

import pandas as pd

from .pg_models import stock
from ._postgres import get_session
from ._third_party_api import get_tushare_data
from ._tool import flat_1dim
from ..interface import AbstractUniverse


@lru_cache()
class StockUniverse(AbstractUniverse):
    def __init__(self, issue_month=12, delist_month=1, no_st=True, no_suspend=True):
        self.issue = issue_month * 31
        self.delist = delist_month * 31
        self.no_st = no_st
        self.no_suspend = no_suspend

    @lru_cache(maxsize=2)
    def get_instruments(self, trade_dt):
        with get_session() as session:
            query_desc = session.query(
                stock.AShareDescription.wind_code,
            ).filter(
                stock.AShareDescription.list_dt < (trade_dt - pd.Timedelta(days=self.issue)),
                stock.AShareDescription.delist_dt > (trade_dt + pd.Timedelta(days=self.delist)),
            ).all()
            query_trade = session.query(
                stock.AShareEODPrice.wind_code
            ).filter(
                stock.AShareEODPrice.trade_dt == trade_dt,
                stock.AShareEODPrice.trade_status == 0
            ).all()
            query_st = pd.DataFrame(
                session.query(
                    stock.ASharePreviousName.wind_code,
                    stock.ASharePreviousName.sector_code
                ).filter(
                    stock.ASharePreviousName.entry_dt <= trade_dt,
                    stock.ASharePreviousName.remove_dt >= trade_dt
                ).all()
            )
            stock_dt = query_st.loc[query_st['new_name'].str.contains(r'ST|PT'), 'wind_code']

        return {*flat_1dim(query_desc)} - {*flat_1dim(query_trade)} - {*stock_dt}


def get_derivative_indicator(trade_dt, codes=None, fields=None):
    data = get_tushare_data(
        api_name='daily_basic',
        trade_date=f'{trade_dt:%Y%m%d}',
        fields=['ts_code', *fields] if fields else None,
        col_mapping={'ts_code': 'wind_code'},
    ).set_index('wind_code')
    if codes:
        data = data.filter(codes, axis=0)
    return data
