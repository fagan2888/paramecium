# -*- coding: utf-8 -*-
"""
@Time: 2020/2/11 15:58
@Author: Sue Zhu
"""
__all__ = ['AbstractFactor', 'AbstractUniverse']

import abc

from .const import FreqEnum
from .utils import camel2snake


class AbstractUniverse(metaclass=abc.ABCMeta):
    """ Universe Interface """

    @abc.abstractmethod
    def get_instruments(self, dt):
        return NotImplementedError


class AbstractFactor(metaclass=abc.ABCMeta):
    asset_type = None

    @property
    def name(self):
        return camel2snake(self.__class__.__name__)

    @abc.abstractmethod
    def compute(self):
        return NotImplementedError


def calculate_and_saving_factor(factor, start_date, end_date, freq=FreqEnum.D):
    pass
