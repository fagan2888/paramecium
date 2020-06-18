# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 9:44
@Author: Sue Zhu
"""
from functools import partial, lru_cache

import numpy as np
import pandas as pd
import scipy.stats as sc_stats

from . import const
from .database import FactorDBTool, get_price
from .interface import AbstractFactor, AbstractUniverse
from .utils import transformer as tf, price_stats as stats


def _get_adj_price(dt, asset_type):
    price = get_price(asset_type, dt, dt).set_index('wind_code')
    if asset_type == const.AssetEnum.STOCK:
        return (price['close_'] * price['adj_factor']).rename('adj_price')
    elif asset_type == const.AssetEnum.CMF:
        return (price['unit_nav'] * price['adj_factor']).rename('adj_price')
    else:
        raise KeyError("Unknown asset type.")


def _cut_or_nan(val, q):
    try:
        return pd.qcut(val, q=q, labels=[f'L{i + 1:02.0f}' for i in range(q)], precision=8).astype(str)
    except ValueError:
        return pd.Series(index=val.index)


class SingleFactorAnalyzer(object):
    """
    单因子分析
    """

    def __init__(
            self, factor: AbstractFactor, transformers=(tf.OutlierMAD(), tf.ScaleNormalize()),
            universe: 'AbstractUniverse' = None,
            ic_method='spearman', group_quantile=5
    ):
        self.obj = factor
        self.io = FactorDBTool(self.obj)

        self.transformers = transformers
        self.universe = universe
        self.ic_method = ic_method
        self.group_quantile = group_quantile

    @lru_cache(maxsize=4)
    def get_price(self, dt):
        return _get_adj_price(dt, asset_type=self.obj.asset_type)

    @lru_cache(maxsize=4)
    def get_factor(self, dt):
        val = self.io.fetch_snapshot(dt)
        if self.universe is not None:
            val = val.filter(self.universe.get_instruments(dt), axis=0)
        return val

    @staticmethod
    def describe_stats(val):
        return pd.DataFrame({k: func(val) for k, func in {
            'mean': partial(np.nanmean, axis=0),
            'std': partial(np.nanstd, ddof=1, axis=0),
            'skewness': partial(sc_stats.skew, axis=0, nan_policy='omit'),
            'kurtosis': partial(sc_stats.kurtosis, axis=0, nan_policy='omit'),
            'min': partial(np.nanmin, axis=0),
            'q1': partial(np.nanpercentile, axis=0, q=25),
            'q2': partial(np.nanpercentile, axis=0, q=50),
            'q3': partial(np.nanpercentile, axis=0, q=75),
            'max': partial(np.nanmax, axis=0),
            'count': pd.DataFrame.count
        }.items()})

    def ic_test(self, val, ret):
        return val.corrwith(ret, axis=0, method=self.ic_method)

    @staticmethod
    def snapshot_reg(val, ret):
        def _agg(arr):
            arr_, ret_ = arr.dropna().align(ret, axis=0, join='inner')
            reg = stats.regression(ret_.values, np.vstack([np.ones_like(arr_), arr_.values]).T)
            return pd.Series([reg.beta[1], reg.t_value[1]], index=['ret', 't'])

        return val.apply(_agg).T

    def run(self, output, start_date=None, end_date=None, freq=const.FreqEnum.M, shift=1):
        dates = [t for t in self.io.get_calc_dates(start_date, end_date, freq)]

        desc, ic, reg, grouped = dict(), dict(), dict(), dict()

        for i, t in enumerate(dates[:-shift]):
            print(f'Run test at {t:%Y-%m-%d}......{i / (len(dates) - shift) * 100:.2f}%')

            # 数据准备
            factor_val = self.get_factor(t)
            if factor_val.shape[0] < self.group_quantile * 1.5:
                continue
            ret = self.get_price(dates[i + shift]).div(self.get_price(dates[i + shift - 1])).sub(1).dropna()
            ret = ret.reindex(index=factor_val.index).fillna(0)

            # 描述性统计
            desc[t] = self.describe_stats(factor_val)

            # 统计后再去极值、标准化
            if self.transformers:
                for trans in self.transformers:
                    factor_val.loc[:] = trans.fit_transform(factor_val.values)

            # 分级靠档
            rank_val = factor_val.apply(_cut_or_nan, q=self.group_quantile).dropna(axis=1, how='all')

            # ic test and reg test
            ic[t] = self.ic_test(factor_val, ret)
            reg[t] = self.snapshot_reg(factor_val, ret)

            # grouped portfolio
            g = lambda ser: ret.groupby(ser.dropna()).mean()
            g_ret = rank_val.apply(g).drop('nan', errors='ignore').T
            grouped[t] = g_ret.assign(DIFF=g_ret[f'L{self.group_quantile:02.0f}'] - g_ret[f'L01'])

        # summary
        with pd.ExcelWriter(output, datetime_format='yyyy/m/d') as excel:
            # info
            pd.Series({
                '因子': self.io.table.key,
                '板块': self.universe,
                '开始日期': min(dates),
                '结束日期': max(dates),
                '频率': freq.name,
                '测试时间': pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, name='单因子测试').to_excel(excel, '基本信息')

            sum_rows = 0
            sum_sheet = '结果'

            # description stats
            desc_summary = pd.concat(desc, names=['trade_dt', 'field_name'])
            desc_summary.reset_index().to_excel(excel, '描述性统计', index=False)

            # ic test
            ic_ts = pd.DataFrame(ic).T
            ic_ts.to_excel(excel, 'RankIC')

            ic_mean = ic_ts.mean()
            ic_summary = pd.DataFrame({
                'IC Mean': ic_mean,
                'IR': ic_mean / ic_ts.std(),
                'T-Stats': sc_stats.ttest_1samp(ic_ts, popmean=0, axis=0, nan_policy='omit').statistic,
                'AutoCorr1': ic_ts.apply(pd.Series.autocorr, lag=1),
                'AutoCorr2': ic_ts.apply(pd.Series.autocorr, lag=2)
            }).T
            ic_summary.to_excel(excel, sum_sheet, startrow=sum_rows)
            sum_rows += ic_summary.shape[0] + 3

            # reg test
            reg_ts = pd.concat(reg, names=['trade_dt', 'field_name'])
            reg_ts.reset_index().to_excel(excel, '截面回归检验_详情', index=False)

            reg_summary = pd.DataFrame({
                'Mean Ret': reg_ts['ret'].unstack('field_name').mean(),
                'T-Test': sc_stats.ttest_1samp(
                    reg_ts['ret'].unstack('field_name'), popmean=0, axis=0, nan_policy='omit').statistic,
                'Mean T-Stat': reg_ts['t'].unstack('field_name').mean(),
                'AutoCorr1': reg_ts['ret'].unstack('field_name').apply(pd.Series.autocorr, lag=1),
                'AutoCorr2': reg_ts['ret'].unstack('field_name').apply(pd.Series.autocorr, lag=2)
            }).T
            reg_summary.to_excel(excel, sum_sheet, startrow=sum_rows)
            sum_rows += ic_summary.shape[0] + 3

            # 分层
            grouped_ts = pd.concat(grouped, names=['trade_dt', 'field_name'])
            grouped_ts.groupby('field_name').apply(
                lambda df: pd.DataFrame(
                    {
                        '年化收益': stats.annual_returns(df, mul=freq.value),
                        '年化波动': stats.annual_volatility(df, mul=freq.value),
                        '最大回撤': stats.max_draw_down(df),
                        '夏普比率': stats.sharpe_ratio(df, mul=freq.value, rf=0)
                    },
                    index=grouped_ts.columns
                )
            ).to_excel(excel, '分层收益', merge_cells=False)
            grouped_ts.reset_index().to_excel(excel, '分层收益_详情', index=False)
