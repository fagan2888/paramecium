# -*- coding: utf-8 -*-
"""
@Time: 2019/8/4 17:36
@Author:  MUYUE1
"""
import abc


class ConstraintABC(metaclass=abc.ABCMeta):

    def __init__(self, is_equal=True):
        self._eq = 'eq' if is_equal else 'ineq'

    @abc.abstractmethod
    def func(self, weights):
        pass

    @property
    def constraint(self):
        return {'type': self._eq, 'fun': self.func}


class WeightConstraint(ConstraintABC):

    def __init__(self, asset_id, bound, is_left):
        super().__init__(is_equal=False)
        self._wind_code = asset_id
        self._bound = bound
        self._is_left = is_left

    def func(self, weights):
        pass
