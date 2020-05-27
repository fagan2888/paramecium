# -*- coding: utf-8 -*-
"""
@Time: 2020/2/11 15:58
@Author: Sue Zhu
"""
__all__ = ['AbstractFactor']

import abc

from .utils import camel2snake


class AbstractFactor(metaclass=abc.ABCMeta):
    asset_type = None

    @property
    def name(self):
        return camel2snake(self.__class__.__name__)

    @abc.abstractmethod
    def compute(self):
        return NotImplementedError


def calculate_and_saving_factor(factor_class, start_date, end_date):
    pass

