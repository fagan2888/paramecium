# -*- coding: utf-8 -*-
"""
@Time: 2020/2/11 15:58
@Author: Sue Zhu
"""
__all__ = ['AbstractFactor', 'AbstractFactorIO', 'AbstractUniverse', 'AbstractTransformer']

import abc
import logging

import pandas as pd

from .const import AssetEnum, FreqEnum
from .utils import camel2snake


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

    def __str__(self):
        return self.__class__.__name__

    def __format__(self, format_spec):
        return self.__str__()

    @property
    def name(self):
        return camel2snake(self.__class__.__name__)

    @property
    @abc.abstractmethod
    def field_types(self):
        return dict()

    @abc.abstractmethod
    def compute(self, dt):
        return pd.DataFrame()


class AbstractFactorIO(metaclass=abc.ABCMeta):

    def __init__(self, factor: AbstractFactor, *args, **kwargs):
        self._factor = factor

    @property
    def logger(self):
        return logging.getLogger(f'{self.__class__.__name__:s}({self._factor!s})')

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
    def get_calc_dates(self, start, end, freq):
        """
        日期序列

        :param start: pd.Timestamp
        :param end: pd.Timestamp or None
        :param freq: FreqEnum
        :return:
        """
        return ()

    def localized_time_series(self, start=None, end=None, freq=FreqEnum.M, if_exist=1):
        for t in self.get_calc_dates(start, end, freq):
            self.localized_snapshot(t, if_exist)
