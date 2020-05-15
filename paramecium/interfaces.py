# -*- coding: utf-8 -*-
"""
@Time: 2020/2/11 15:58
@Author: Sue Zhu
"""
__all__ = ['FactorInterface']

import abc

from paramecium.utils import camel2snake


class FactorInterface(metaclass=abc.ABCMeta):

    @property
    def name(self):
        return camel2snake(self.__class__.__name__)

    @abc.abstractmethod
    def compute(self):
        pass
