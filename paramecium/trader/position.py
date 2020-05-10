# -*- coding: utf-8 -*-
"""
@Time: 2019/8/11 19:26
@Author:  MUYUE1
"""
from dataclasses import dataclass
import numpy as np
from .trade import Trade


@dataclass
class Position(object):
    wind_code: str
    amount: float = 0.
    price: float = np.nan

    avg_price: float = 0  # average hold price
    transaction: float = 0  # transaction fee

    @property
    def market_value(self):
        return self.price * self.amount

    @property
    def pnl(self):
        return self.amount * (self.price - self.avg_price)

    def apply_trade(self, trade: Trade):
        if trade.amount > 0:
            self.avg_price = (self.market_value + trade.price * trade.amount) / (self.amount + trade.amount)
        self.price = trade.price
        self.amount += trade.amount
        self.transaction += trade.transaction


class Positions(dict):

    def __missing__(self, wind_code):
        self[wind_code] = Position(wind_code)
        return self[wind_code]
