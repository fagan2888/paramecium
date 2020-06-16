# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 9:44
@Author: Sue Zhu
"""
import pandas as pd

from . import const
from .database import FactorDBTool, get_price
from .interface import AbstractFactor, AbstractUniverse


def _get_adj_price(asset_type, dt):
    price = get_price(asset_type, dt, dt).set_index('wind_code')
    if asset_type == const.AssetEnum.STOCK:
        return (price['close_'] * price['adj_factor']).rename('adj_price')
    elif asset_type == const.AssetEnum.CMF:
        return (price['unit_nav'] * price['adj_factor']).rename('adj_price')
    else:
        raise KeyError("Unknown asset type.")


def single_factor_analysis(factor: AbstractFactor, transformers=None,
                           start_date=None, end_date=None, freq=const.FreqEnum.M, shift=1,
                           universe: 'AbstractUniverse' = None,
                           ic_method='spearman', quantile=5, benchmark='000300.XSHG'):
    """
    单因子分析
    """
    factor_io = FactorDBTool(factor)
    dates = [t for t in factor_io.get_calc_dates(start_date, end_date, freq)]

    # data prepare
    adj_price = pd.concat((t for t in dates), axis=0).pivot('trade_dt', 'wind_code', 'adj_price')
    ret = adj_price.pct_change(1, limit=1).slice_shift(-shift).iloc[:-shift]
    factor_val = {t: factor_io.fetch_snapshot(t) for t in dates[:-shift]}

    for t, val in factor_val.items():
        # 描述性统计
        val.describe()
