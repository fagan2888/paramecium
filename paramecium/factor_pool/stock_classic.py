# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 15:23
@Author: Sue Zhu
"""
import numpy as np
import pandas as pd

from paramecium.interface import AbstractFactor
from paramecium.database import StockUniverse
from paramecium.utils.data_api import get_tushare_api


class StockFamaFrench(AbstractFactor):
    """ Classic Fama-French 3 Factor """

    def __init__(self):
        self.universe = StockUniverse(issue_month=12, no_st=True, no_suspend=True)

    def get_empty_table(self):
        return pd.DataFrame(
            np.nan, columns=['smb', 'hml', 'capt']
        ).assign(wind_code='', trade_dt=pd.Timestamp.now(), label='')

    def compute(self, dt):
        universe = self.universe.get_instruments(dt)
        derivative = get_tushare_api().daily_basic(
            trade_date=f'{dt:%Y%m%d}',
            fields="ts_code,tot_mv,mv,pb_new"
        ).rename(columns={
            'ts_code': 'wind_code',
            'tot_mv': 'capt', 'mv': 'size',  # 万元
            'pb_new': 'value',
        }).set_index('wind_code').filter(universe)
        derivative.loc[:, 'size'] = np.log(derivative.loc[:, 'size'])
        derivative.loc[:, 'value'] = 1/derivative.loc[:, 'value']
