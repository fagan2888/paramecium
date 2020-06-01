# -*- coding: utf-8 -*-
"""
@Time: 2020/2/11 15:58
@Author: Sue Zhu
"""
__all__ = ['AbstractFactor', 'AbstractFactorIO', 'AbstractUniverse', 'AbstractTransformer']

import abc
import logging

import pandas as pd

from paramecium.const import AssetEnum, FreqEnum
from paramecium.utils import camel2snake


class AbstractTransformer(metaclass=abc.ABCMeta):

    def fit(self, raw_data):
        return self

    @abc.abstractmethod
    def transform(self, raw_data):
        return NotImplementedError

    def fit_transform(self, raw_data):
        return self.fit(raw_data).transform(raw_data)


class AbstractUniverse(metaclass=abc.ABCMeta):
    """ Universe Interface """

    @abc.abstractmethod
    def get_instruments(self, dt):
        return NotImplementedError


class AbstractFactor(metaclass=abc.ABCMeta):
    asset_type: AssetEnum = None
    start_date = pd.Timestamp.min

    @property
    def name(self):
        return camel2snake(self.__class__.__name__)

    @property
    def field_types(self):
        return dict()

    @abc.abstractmethod
    def get_empty_table(self):
        return pd.DataFrame()

    @abc.abstractmethod
    def compute(self, dt):
        return pd.DataFrame()


class AbstractFactorIO(metaclass=abc.ABCMeta):

    def __init__(self, factor: 'AbstractFactor', *args, **kwargs):
        self._factor = factor

    @property
    def logger(self):
        return logging.getLogger(f'{self.__class__.__name__:s}({self._factor.__class__.__name__:s})')

    @property
    def table_name(self):
        return f"{self._factor.asset_type.value}_factor_{self._factor.name}"

    @abc.abstractmethod
    def localized_snapshot(self, dt, if_exist=1):
        """
        计算并保存截面数据
        :param dt: pd.Timestamp
        :param if_exist: {1: replace, 0: error}, 由于因子在截面上有相关性，不考虑update部分的情况
        :return:
        """
        return NotImplementedError

    @abc.abstractmethod
    def fetch_snapshot(self, dt):
        """
        读取截面数据
        :param dt: pd.Timestamp
        :return:
        """
        return ()

    @abc.abstractmethod
    def _get_dates(self, start, end, freq):
        """
        日期序列
        :param start: pd.Timestamp
        :param end: pd.Timestamp or None
        :param freq: FreqEnum
        :return:
        """
        return ()

    def localized_time_series(self, start=None, end=None, freq=FreqEnum.M, if_exist=1):
        for t in self._get_dates(start, end, freq):
            self.localized_snapshot(t, if_exist)

    def fetch_time_series(self, start=None, end=None, freq=FreqEnum.M):
        return pd.concat(
            {t: self.fetch_snapshot(t) for t in self._get_dates(start, end, freq)},
            axis=0, sort=False, names=['trade_dt', 'wind_code']
        )
