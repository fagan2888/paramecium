# -*- coding: utf-8 -*-
"""
@Time: 2019/7/15 23:03
@Author: Sue Zhu
"""
import abc

from .event import EventType
from .environment import Enviroment


class AbstractStrategy(metaclass=abc.ABCMeta):

    def __init__(self, register_event=True):
        pass

    def _register_event(self):
        env = Enviroment.get_instance()

        env.event_bus.register_listener(EventType.INIT, self.init)
        env.event_bus.register_listener(EventType.ON_TRADING, self.on_trading)
        env.event_bus.register_listener(EventType.BEFORE_TRADING, self.before_trading)
        env.event_bus.register_listener(EventType.AFTER_TRADING, self.after_trading)

    @abc.abstractmethod
    def init(self):
        pass

    @abc.abstractmethod
    def on_trading(self):
        pass

    def before_trading(self):
        pass

    def after_trading(self):
        pass
