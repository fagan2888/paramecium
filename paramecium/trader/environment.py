# -*- coding:utf-8 -*-
"""
Created at 2019/7/6 by Sue.

Usage:
    
"""
from .event import EventBus, EventType

import typing

if typing.TYPE_CHECKING:
    from .account import AbstractAccount
    from .data_proxy import AbstractDataProxy
    from .strategy import AbstractStrategy


class Enviroment(object):
    _env = None

    def __init__(self):
        Enviroment._env = self

        self.data_proxy: 'AbstractDataProxy' = None

        self.pre_dt = None
        self.cur_dt = None
        self.event_bus = EventBus()

        self.accounts = dict()
        self.orders = dict()
        self.trades = dict()

    @classmethod
    def get_instance(cls):
        if cls._env is None:
            raise RuntimeError("Environment has not been initialize.")
        return cls._env

    def set_data_proxy(self, proxy: 'AbstractDataProxy'):
        self.data_proxy = proxy

    def get_history_price(self, asset, n, freq='D'):
        start = self.data_proxy.get_date_offset(self.cur_dt, n - 1, freq, backward=True)
        return self.data_proxy.get_history_price(asset, start, self.cur_dt, freq)
