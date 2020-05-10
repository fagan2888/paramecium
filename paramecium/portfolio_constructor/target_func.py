# -*- coding: utf-8 -*-
"""
@Time: 2019/8/4 17:20
@Author:  MUYUE1
"""
import abc


class TargetABC(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def optimize(self, weights):
        pass


class MinVariance(TargetABC):

    def __init__(self, freq='D', count=252):
        self._freq = freq
        self._count = count

    def optimize(self, weights):
        return


class MaxProfit(TargetABC):

    def __init__(self, freq='D', count=252):
        self._freq = freq
        self._count = count

    def optimize(self, weights):
        return


class MaxSharpeRatio(TargetABC):

    def __init__(self, freq='D', count=252, rf=0.):
        self._freq = freq
        self._count = count
        self._rf = rf

    def optimize(self, weights):
        return


class MinTrackingError(TargetABC):

    def __init__(self, benchmark_id, freq='D', count=252):
        self._benchmark_id = benchmark_id
        self._freq = freq
        self._count = count

    def optimize(self, weights):
        return


class RiskParity(TargetABC):

    def __init__(self, risk_budget=None, freq='D', count=252):
        self._risk_budget = risk_budget
        self._freq = freq
        self._count = count

    def optimize(self, weights):
        return
