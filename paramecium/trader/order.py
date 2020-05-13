# -*- coding: utf-8 -*-
"""
@Time: 2019/7/11 22:00
@Author: Sue Zhu
"""
from dataclasses import dataclass, field
from functools import wraps
from uuid import uuid1, UUID

import pandas as pd

from .const import CustomEnum
from .environment import Enviroment


def _no_deal(func):
    @wraps(func)
    def wrapper(self):
        try:
            return func(self)
        except TypeError:
            return 0

    return wrapper


class OrderType(CustomEnum):
    NEW = 'new'
    DEAL = 'deal'
    CANCEL = 'cancel'
    REJECTED = 'rejected'


@dataclass
class Order(object):
    __env = property(lambda self: Enviroment.get_instance())
    order_id: UUID = field(default_factory=uuid1)

    instrument_id: str = ''
    creat_at: pd.Timestamp = pd.Timestamp.max
    amount: float = 0
    is_long: bool = True

    status: OrderType = OrderType.NEW
    _trades: list = field(default_factory=list)
    trades = property(lambda self: (self.__env.get(_id) for _id in self._trades))
    reject_reason: str = ''

    def __post_init__(self):
        self.__env[self.instrument_id] = self

    @property
    @_no_deal
    def filled(self):
        return sum(trade.amount for trade in self.trades)

    @property
    @_no_deal
    def avg_price(self):
        return sum(trade.price * trade.amount for trade in self.trades) / self.filled


