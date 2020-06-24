# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 15:23
@Author: Sue Zhu
"""
from functools import partial

import numpy as np
import pandas as pd

from ..const import AssetEnum
from ..database import stock_
from ..interface import AbstractFactor
from ..utils.transformer import OutlierMAD, ScaleMinMax


class FamaFrench(AbstractFactor):
    """
    Classic Fama-French 3 Factor
    """
    asset_type = AssetEnum.STOCK
    start_date = pd.Timestamp('2003-12-31')

    def __init__(self):
        self.universe = stock_.StockUniverse()
        self.value_scale = ScaleMinMax(
            min_func=partial(np.nanpercentile, q=30, axis=0),
            max_func=partial(np.nanpercentile, q=70, axis=0)
        ).fit_transform

    @property
    def field_types(self):
        return dict(size=float, value=float, capt=float, label=str)

    def compute(self, dt):
        # stock match case
        universe = self.universe.get_instruments(dt)

        # get derivative data
        derivative = stock_.get_derivative_indicator(
            trade_dt=f'{dt:%Y%m%d}',
            fields=['mv', 'pe_ttm'],
        ).rename(columns={'mv': 'capt'}).filter(universe, axis=0).fillna({'pe_ttm': 0})

        # calculate and clean outliers.
        derivative['size'] = np.log10(derivative['capt'])
        derivative['value'] = 1 / derivative['pe_ttm']
        derivative.loc[:, ['size', 'value']] = OutlierMAD().fit_transform(derivative.loc[:, ['size', 'value']].values)

        # re scale data with robust-scale to make style identify easier
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

        return derivative.filter(self.field_types.keys(), axis=1)
