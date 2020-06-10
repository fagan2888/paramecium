# -*- coding: utf-8 -*-
"""
@Time: 2020/6/9 14:27
@Author: Sue Zhu
"""
from functools import partial

import numpy as np
import pandas as pd
import scipy.stats as sc_stats

from .. import const
from ..database import get_dates, get_price, get_index_ff3, get_index_bond5
from ..database.fund import FundUniverse
from ..interface import AbstractFactor
from ..utils import price_stats as p_stats


class _PriceRelated(AbstractFactor):
    asset_type = const.AssetEnum.CMF
    std_limit = 1e-8
    start_date = pd.Timestamp('2009-12-31')
    universe = FundUniverse(issue_month=0, size_=0)

    def __init__(self, bk_win, freq=const.FreqEnum.W):
        self.freq = freq
        self.bk_win = bk_win

    @property
    def name(self):
        return self.freq.name.lower() + (f'{self.bk_win:03d}' if self.bk_win else "_itd")

    @classmethod
    def _get_price_pvt(cls, dt, bk_win=None, freq=const.FreqEnum.W):
        dates = [t for t in get_dates(freq) if t <= dt]
        funds = cls.universe.get_instruments(dt)

        price = get_price(cls.asset_type, start=dates[-bk_win - 1], end=dt)
        price['adj_nav'] = price['unit_nav'] * price['adj_factor']
        price_pvt = price.pivot('trade_dt', 'wind_code', 'adj_nav').filter(dates, axis=0).filter(funds, axis=1)
        ret = price_pvt.pct_change(1, limit=1).iloc[1:].where(lambda df: df.ne(0))
        if bk_win is not None:
            ret = ret.dropna(thresh=round(bk_win * 0.8), axis=1)
        ret = ret.loc[:, ret.std().gt(cls.std_limit) & ret.abs().max().le(1.1 ** (250 / freq.value) - 1)]
        return ret


class FundPerform(_PriceRelated):

    def __init__(self, bk_win, freq=const.FreqEnum.W):
        super().__init__(bk_win=bk_win, freq=freq)
        _rf = 0
        _alpha = 0.05
        self.funcs = dict(
            ann_ret=partial(p_stats.annual_returns, mul=self.freq.value),
            ann_vol=partial(p_stats.annual_volatility, mul=self.freq.value),
            skewness=partial(sc_stats.skew, axis=0, nan_policy='omit'),
            kurtosis=partial(sc_stats.kurtosis, axis=0, nan_policy='omit'),
            max_dd=partial(p_stats.max_draw_down),
            down_side_risk=partial(p_stats.down_side_risk, rf=_rf, mul=self.freq.value),
            var=partial(p_stats.value_at_risk, alpha=_alpha),
            c_var=partial(p_stats.conditional_var, alpha=_alpha),
            sharpe=partial(p_stats.sharpe_ratio, mul=self.freq.value, rf=_rf),
            sortino=partial(p_stats.sortino_ratio, mul=self.freq.value, rf=_rf),
            calmar=partial(p_stats.calmar_ratio, mul=self.freq.value, rf=_rf),
        )

    @property
    def field_types(self):
        return {k: float for k in self.funcs.keys()}

    @property
    def name(self):
        return f'perform_{super().name}'

    def compute(self, dt):
        fund_ret = self._get_price_pvt(dt, self.bk_win, self.freq)
        factor = pd.DataFrame(
            {name: func(fund_ret) for name, func in self.funcs.items()},
            index=fund_ret.columns
        )
        factor = factor.where(~np.isinf(factor))
        return factor


class _Reg(_PriceRelated):

    def __init__(self, bk_win, freq=const.FreqEnum.W):
        super().__init__(bk_win=bk_win, freq=freq)
        self.index_ret = self.get_index_ret()

    def get_index_ret(self):
        return pd.DataFrame()

    @property
    def field_types(self):
        cols = self.index_ret.columns
        return {k: float for k in (*cols, *(f'{c}_tstats' for c in cols), 'r2')}

    def compute(self, dt):
        fund_ret = self._get_price_pvt(dt, self.bk_win, self.freq)
        fund_ret, idx = fund_ret.fillna(0).align(self.index_ret, axis=0, join='inner')
        if idx.shape[0] < self.bk_win * 0.9:
            return pd.DataFrame(columns=self.field_types.keys())
        else:
            factor = p_stats.regression(fund_ret, idx)
            return factor


class FundRegFF3(_Reg):

    def get_index_ret(self):
        return get_index_ff3(calc_freq=self.freq).assign(alpha=1)

    @property
    def name(self):
        return f'reg_ff3_{super().name}'


class FundRegBond5(_Reg):

    def get_index_ret(self):
        return get_index_bond5(calc_freq=self.freq).assign(alpha=1)

    @property
    def name(self):
        return f'reg_bond5_{super().name}'
