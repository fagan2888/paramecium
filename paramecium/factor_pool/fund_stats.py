# -*- coding: utf-8 -*-
"""
@Time: 2020/6/9 14:27
@Author: Sue Zhu
"""
__all__ = ['FundPerform', 'FundRegFF3', 'FundRegBond5']

from functools import partial

import numpy as np
import pandas as pd
import scipy.stats as sc_stats

from .. import const
from ..database import get_dates, get_price, get_index_ff3, get_index_bond5
from ..database.fund_ import FundUniverse
from ..interface import AbstractFactor
from ..utils import price_stats as p_stats


class _RetFactor(AbstractFactor):
    """
    收益率相关因子抽象类，统一基金初筛逻辑及收益计算方式
    """
    asset_type = const.AssetEnum.CMF
    std_limit = 1e-8  # 去除净值为一条线的情况
    start_date = pd.Timestamp('2009-12-31')

    def __init__(self, bk_win, freq='W'):
        self.freq = const.FreqEnum[freq]
        self.bk_win = bk_win
        self.universe = FundUniverse(**self._universe_param)

    def __str__(self):
        return f'{super().__str__()}(bk_win={self.bk_win}, freq={self.freq.name})'

    @property
    def name(self):
        return f'{self.freq.name.lower()}{self.bk_win:03d}'

    @property
    def _universe_param(self):
        return dict(
            exclude_=None,
            issue=int(250 / self.freq.value * self.bk_win) + 63,  # 考虑到至少三个月建仓期
            size_=0,  # 初始条件较为宽松，防止因子覆盖率过低
            manager=0
        )

    def _get_ret_pvt(self, dt):
        dates = [t for t in get_dates(self.freq) if t <= dt]
        funds = self.universe.get_instruments(dt)

        price = get_price(self.asset_type, start=dates[-self.bk_win - 1], end=dt)
        price['adj_nav'] = price['unit_nav'] * price['adj_factor']
        price_pvt = price.pivot('trade_dt', 'wind_code', 'adj_nav').filter(dates, axis=0).filter(funds, axis=1)
        ret = price_pvt.pct_change(1, limit=1).iloc[1:].where(lambda df: df.ne(0))
        ret = ret.dropna(thresh=round(self.bk_win * 0.8), axis=1)
        ret = ret.loc[:, ret.std().gt(self.std_limit) & ret.abs().max().le(1.1 ** (250 / self.freq.value) - 1)]
        return ret


class FundPerform(_RetFactor):
    """
    基金收益统计指标

        - ann_ret: 年化收益
        - ann_vol: 年化波动
        - skewness: 偏度
        - kurtosis: 峰度
        - max_dd: 最大回撤
        - down_side_risk: 下行风险
        - var: 在险价值
        - c_var: 条件在险价值
        - sharpe: 夏普比率
        - sortino: 索提诺比率
        - calmar: 卡玛比率
    """

    def __init__(self, bk_win, freq='W'):
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
    def name(self):
        return f'perform_{super().name}'

    @property
    def field_types(self):
        return {k: float for k in self.funcs.keys()}

    def compute(self, dt):
        fund_ret = self._get_ret_pvt(dt)
        factor = pd.DataFrame(
            {name: func(fund_ret) for name, func in self.funcs.items()},
            index=fund_ret.columns
        )
        factor = factor.where(~np.isinf(factor))
        return factor


class _Reg(_RetFactor):
    """"
    回归因子
    """

    def __init__(self, bk_win, freq='W', half_life=0):
        super().__init__(bk_win=bk_win, freq=freq)
        self.index_ret = self.get_index_ret()
        self.half_life = half_life

    def __str__(self):
        half = f', half={self.half_life}' if self.half_life else ''
        return f'{super().__str__()[:-1]}{half})'

    @property
    def name(self):
        return f'{super().name}' + (f'_half{self.half_life}' if self.half_life else '')

    def get_index_ret(self):
        return pd.DataFrame()

    @property
    def field_types(self):
        cols = self.index_ret.columns
        return {k: float for k in (*cols, *(f'{c}_tstats' for c in cols), 'r2')}

    def compute(self, dt):
        fund_ret = self._get_ret_pvt(dt)
        fund_ret, idx = fund_ret.fillna(0).align(self.index_ret, axis=0, join='inner')
        if idx.shape[0] < self.bk_win * 0.9:
            return pd.DataFrame(columns=self.field_types.keys())
        else:
            reg = p_stats.regression(fund_ret.values, idx.values)
            factor = pd.DataFrame(
                np.hstack([reg.beta, reg.t_value, reg.r2]),
                columns=self.field_types.keys(),
                index=fund_ret.columns
            )
            return factor.round(8)


class FundRegFF3(_Reg):

    def __init__(self, bk_win, freq='W', half_life=None, timing=None):
        self.timing = timing
        super().__init__(bk_win, freq, half_life)

    def __str__(self):
        return f'{super().__str__()[:-1]}, timing={self.timing})'

    def get_index_ret(self):
        return get_index_ff3(calc_freq=self.freq, timing=self.timing).assign(alpha=1)

    @property
    def name(self):
        return f'reg_ff3{"_" + self.timing if self.timing else ""}_{super().name}'


class FundRegBond5(_Reg):

    def get_index_ret(self):
        return get_index_bond5(calc_freq=self.freq).assign(alpha=1)

    @property
    def name(self):
        return f'reg_bond5_{super().name}'


class MoneyFundNonTradeRet(_RetFactor):
    date_mapper = None

    def __init__(self, bk_win):
        super().__init__(bk_win, freq='D')

    @property
    def name(self):
        return f'cmoney_notrd_{super().name}'

    @property
    def field_types(self):
        return {'avg_ret': float}

    @property
    def _universe_param(self):
        return {'include_': ('2001010400000000',), 'exclude_': ("1000033974000000",), **super()._universe_param}

    def compute(self, dt):
        idx = get_dates(self.freq).to_series()
        non_trd_dt = (idx - idx.slice_shift(1)).dt.days.loc[lambda ser: ser.gt(1)]
        non_trd_ret = self._get_ret_pvt(dt)
        non_trd_ret, non_trd_dt = non_trd_ret.align(non_trd_dt, axis=0, join='inner')
        result = pd.Series(
            np.average(non_trd_ret.div(non_trd_dt, axis=0).fillna(0), axis=0, weights=non_trd_dt - 1) * 1e4,
            index=non_trd_ret.columns
        )
        return result.to_frame('avg_ret')
