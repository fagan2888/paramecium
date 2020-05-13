# -*- coding: utf-8 -*-
"""
@Time: 2019/8/11 19:24
@Author: Sue Zhu
"""
from dataclasses import dataclass, field
from functools import wraps

import pandas as pd
from bson import ObjectId

from .const import OrderType


@dataclass
class Trade(object):
    deal_at: pd.Timestamp
    amount: float
    price: float
    transaction: float = 0
    cost = property(lambda self: self.amount * self.price + self.transaction)


def _no_trade(func):
    @wraps(func)
    def wrapper(self):
        try:
            return func(self)
        except TypeError:
            return 0

    return wrapper


@dataclass
class Order(object):
    create_at: pd.Timestamp
    wind_code: str
    amount: float

    order_id: ObjectId = field(default_factory=ObjectId)
    status: OrderType = OrderType.NEW
    trades: list = field(default_factory=list)
    reason: str = ''

    @property
    @_no_trade
    def filled(self):
        return sum(t.amount for t in self.trades)

    @property
    @_no_trade
    def transaction(self):
        return sum(t.transaction for t in self.trades)

    @property
    @_no_trade
    def avg_price(self):
        return sum(t.price * t.amount for t in self.trades) / self.filled
