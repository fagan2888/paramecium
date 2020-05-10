# -*- coding:utf-8 -*-
"""
Created at 2019/7/7 by Sue.

Usage:
    
"""
from collections import defaultdict

from .const import CustomEnum


class Event(object):

    def __init__(self, event_type, **kwargs):
        self.event_type = event_type
        self.__dict__.update(kwargs)


class EventType(CustomEnum):
    INIT = 'init'

    BEFORE_TRADING_PRE = 'before_trading_pre'
    BEFORE_TRADING = 'before_trading'
    BEFORE_TRADING_POST = 'before_trading_post'
    ON_TRADING = 'on_trading'
    AFTER_TRADING_PRE = 'after_trading_pre'
    AFTER_TRADING = 'after_trading'
    AFTER_TRADING_POST = 'after_trading_post'

    ORDER_NEW = 'order_new'
    ORDER_TRADE = 'order_trade'
    ORDER_CANCEL = 'order_cancel'
    ORDER_DEAL = 'order_deal'

    SETTLEMENT = 'settlement'


class EventBus(object):

    def __init__(self):
        self._listener = defaultdict(list)

    def register_listener(self, event_type, listener, prepend=False):
        if prepend:
            self._listener[event_type].insert(listener, 0)
        else:
            self._listener[event_type].append(listener)

    def publish_event(self, event: 'Event'):
        for listener in self._listener[event.event_type]:
            if listener(event):
                print(f'Event `{event.event_type}` end at {listener.__name__}.')
                break
