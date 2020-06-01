# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 9:48
@Author: Sue Zhu
"""
import abc


class Account(object):

    def __init__(self, cash=1e4, position=None):
        self.cash = cash
        self.position = dict() if position is None else position
        self.nav = dict()


class SimpleBacktest(metaclass=abc.ABCMeta):

    def __init__(self):
        self.pre_dt, self.cur_dt = None, None
        self.accounts = dict()

    def run(self, start, end, freq):
        pass

    @abc.abstractmethod
    def run_snapshot(self):
        pass
