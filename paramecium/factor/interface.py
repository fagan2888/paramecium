# -*- coding: utf-8 -*-
"""
@Time: 2020/2/11 15:58
@Author:  MUYUE1
"""
__all__ = ['FactorInterface']

import abc

from paramecium.utils import camel2snake


class FactorInterface(metaclass=abc.ABCMeta):

    def compute(self):
        pass
