# -*- coding:utf-8 -*-
"""
Created at 2019/7/6 by Sue.

Usage:
    
"""
import abc
import pandas as pd
from .environment import Enviroment


class AbstractAccount(metaclass=abc.ABCMeta):

    def __init__(self, init_cash):
        self._cash = init_cash
        self._positions = dict()
        self._orders = list()
        # self.register_event()
