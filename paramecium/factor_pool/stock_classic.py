# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 15:23
@Author: Sue Zhu
"""
from functools import partial

import numpy as np
import pandas as pd
import sqlalchemy as sa
from paramecium.const import AssetEnum
from paramecium.database import StockUniverse
from paramecium.interface import AbstractFactor
from paramecium.utils.data_api import get_tushare_api
from paramecium.utils.data_proc import ScaleNormalize, OutlierMAD, ScaleMinMax


class FamaFrench(AbstractFactor):
    """
    Classic Fama-French 3 Factor
    """
    asset_type = AssetEnum.STOCK
    start_date = pd.Timestamp('2003-12-31')

    def __init__(self):
        self.universe = StockUniverse(issue_month=12, no_st=True, no_suspend=True)
        self.value_scale = ScaleMinMax(
            min_func=partial(np.nanpercentile, q=30, axis=0),
            max_func=partial(np.nanpercentile, q=70, axis=0)
        ).fit_transform

    @property
    def field_types(self):
        return dict(label=sa.String(2), **super().field_types)

    def get_empty_table(self):
        return pd.DataFrame(
            np.nan, columns=['size', 'value', 'capt'], index=[]
        ).assign(wind_code='', trade_dt=pd.Timestamp.now(), label='')

    def compute(self, dt):
        # stock match case
        universe = self.universe.get_instruments(dt)
        # sector = get_sector(self.asset_type, valid_dt=dt, sector_type=SectorEnum.STOCK_SEC_ZZ).set_index('wind_code')
        # sector = sector['sector_code'].map(lambda x: x[:4] if x else x).filter(universe, axis=0)

        # get derivative data
        derivative = get_tushare_api().daily_basic(
            trade_date=f'{dt:%Y%m%d}',
            fields="ts_code,mv,pb_new"
        ).rename(
            columns={'ts_code': 'wind_code', 'mv': 'capt'}
        ).set_index('wind_code').filter(universe, axis=0).dropna()  # .reindex(index=sector.index)

        # size value is log10(mv)
        derivative['size'] = np.log10(derivative['capt'])
        derivative['value'] = 1 / derivative['pb_new']
        for proc in (OutlierMAD(), ScaleNormalize()):
            derivative.loc[:, ['size', 'value']] = proc.fit_transform(derivative.loc[:, ['size', 'value']].values)
        # derivative = derivative.fillna(derivative.groupby(sector)[['size', 'value']].mean())

        # re scale data to make style identify easier.
        # use robust scale method and cut point from https://mp.weixin.qq.com/s/2m7JXy2RdbRA1SKgFORgKA
        derivative.loc[:, 'size'] = ScaleMinMax(
            min_func=lambda arr: derivative['size'].sort_values().iloc[-500],
            max_func=lambda arr: derivative['size'].sort_values().iloc[-200]
        ).fit_transform(derivative.loc[:, 'size']) * 200 - 100
        derivative['label'] = pd.cut(derivative['size'], bins=[-np.inf, -100, 100, np.inf], labels=list('SMB'))
        # value label should be neutralized by size
        derivative.loc[:, 'value'] = derivative.groupby('label')['value'].apply(self.value_scale) * 200 - 100
        derivative['label'] = derivative['label'].str.cat(
            pd.cut(derivative['value'], bins=[-np.inf, -100, 100, np.inf], labels=list('GNV'))
        )

        return derivative.filter(self.get_empty_table().columns, axis=1)
