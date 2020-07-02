# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:51
@Author: Sue Zhu
"""
from functools import partial

import numpy as np

from ..interface import AbstractTransformer


class ScaleMinMax(AbstractTransformer):
    __slots__ = ['_min', '_max']

    def __init__(self, min_func=None, max_func=None):
        self._min_func = partial(np.nanmin, axis=0) if min_func is None else min_func
        self._max_func = partial(np.nanmax, axis=0) if max_func is None else max_func
        self._min, self._max = None, None

    def fit(self, raw_data):
        self._min = self._min_func(raw_data)
        self._max = self._max_func(raw_data)
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
    https://www.rdocumentation.org/packages/stats/versions/3.5.2/topics/mad

    The default constant = 1.4826 (approximately \(1/\Phi^{-1}(\frac 3 4)\) = 1/qnorm(3/4)) ensures consistency,
    i.e., $$E[mad(X_1,\dots,X_n)] = \sigma$$ for \(X_i\) distributed as \(N(\mu, \sigma^2)\) and large \(n\).
    """
    __slots__ = ['_is_drop', '_median', '_mad']

    def __init__(self, drop=False, constant=1.4826):
        self._is_drop = drop
        self._const = constant
        self._median = None
        self._mad = None

    @property
    def _sigma(self):
        return self._mad * self._const

    def fit(self, raw_data):
        self._median = np.nanmedian(raw_data, axis=0)
        self._mad = np.nanmedian(np.abs(raw_data - self._median), axis=0)
        return self

    def transform(self, raw_data):
        return np.hstack([self._map_row(*kws).reshape((-1, 1)) for kws in zip(self._median, self._mad, raw_data.T)])

    @staticmethod
    def _min_max(arr):
        if arr.shape[0]:
            return ScaleMinMax().fit_transform(arr)
        else:
            return arr

    def _map_row(self, median, mad, row):
        sigma = mad * 1.4826
        lower_bound, upper_bound = median - 3 * sigma, median + 3 * sigma

        if self._is_drop:
            row[row <= lower_bound] = np.nan
            row[row >= upper_bound] = np.nan
        else:
            rank_row = np.arange(row.shape[0])[np.argsort(row)]
            row[row <= lower_bound] = lower_bound + 0.5 * sigma * (1 - self._min_max(rank_row[row <= lower_bound]))
            row[row >= upper_bound] = upper_bound + 0.5 * sigma * self._min_max(rank_row[row >= upper_bound])
        return row
