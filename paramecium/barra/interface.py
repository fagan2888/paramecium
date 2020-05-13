# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 12:11
@Author: Sue Zhu
"""
import abc
from dataclasses import dataclass

import numpy as np
import pandas as pd


class Descriptor(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def compute(self, dt):
        return pd.DataFrame()


@dataclass
class FactorComponent(object):
    descriptor: Descriptor
    weight: float


class StyleFactor(object):

    def __init__(self, *factor_components:tuple):
        self._components = factor_components

    def compute(self, dt):
        return np.vstack((d * w for d, w in self._components))
