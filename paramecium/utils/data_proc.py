# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:51
@Author: Sue Zhu
"""
import numpy as np

from paramecium.interface import AbstractTransformer


class ScaleMinMax(AbstractTransformer):
    __slots__ = ['_min', '_max']

    def __init__(self):
        self._min = None
        self._max = None

    def fit(self, raw_data):
        self._min = np.nanmin(raw_data, axis=0)
        self._max = np.nanmax(raw_data, axis=0)
        return self

    def transform(self, raw_data):
        return (raw_data - self._min) / (self._max - self._min)


class ScaleNormalize(AbstractTransformer):
    __slots__ = ['_mu', '_sigma']

    def __init__(self):
        self._mu = None
        self._sigma = None

    def fit(self, raw_data):
        self._mu = np.nanmean(raw_data, axis=0)
        self._sigma = np.nanstd(raw_data, ddof=1, axis=0)
        return self

    def transform(self, raw_data):
        return (raw_data - self._mu) / self._sigma


class OutlierMAD(AbstractTransformer):
    """
    Clean Outlier with `Median Absolute Deviation(MAD)`
    """

    __slots__ = ['_is_drop', '_median', '_mad']

    def __init__(self, drop=False):
        self._is_drop = drop
        self._median = None
        self._mad = None

    @property
    def _sigma(self):
        return self._mad * 1.4826

    def fit(self, raw_data):
        self._median = np.nanmedian(raw_data, axis=0)
        self._mad = np.nanmedian(np.abs(raw_data - self._median), axis=0)
        return self

    # def transform(self, raw_data):
    #     for col in


# def outlier_mad(raw_factor):
#     """
#     Clean Outlier with `Median absolute deviation`
#     :param raw_factor: 1d array
#     :return:
#     """
#     # excess values - median absolute deviation (MAD)
#     # p.s. in scipy 1.3+ can use scipy.stats.median_absolute_deviation for calculate mad
#     factor_median = np.nanmedian(raw_factor)
#     mad = np.nanmedian(np.abs(raw_factor - factor_median))
#
#     sigma = 1.4826 * mad
#     ranking = np.arange(raw_factor.shape[0])[raw_factor.argsort()]
#
#     upper_bound = factor_median + 3 * sigma
#     lower_bound = factor_median - 3 * sigma
#     upper = ranking[raw_factor > upper_bound]
#     upper -= upper.min() + 1
#     lower = ranking[raw_factor < lower_bound]
#     # new_factor.loc[raw_factor > upper_bound] = raw_factor.loc[raw_factor > upper_bound].rank(ascending=True,
#     #                                                                                          pct=True) * 0.5 * sigma + upper_bound
#     # new_factor.loc[raw_factor < lower_bound] = lower_bound - raw_factor.loc[raw_factor < lower_bound].rank(
#     #     ascending=False, pct=True) * 0.5 * sigma
#     # return new_factor
