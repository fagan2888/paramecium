# -*- coding: utf-8 -*-
"""
@Time: 2020/5/30 22:42
@Author: Sue Zhu
"""
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd
from paramecium.const import AssetEnum
from paramecium.database.comment import get_price
from paramecium.database._postgres import get_dates
import abc


@dataclass
class FastAccount(object):
    market_value: dict
    cash: dict
    holding: dict

    @property
    def cur_hold(self):
        max_dt = self.holding.get('trade_dt', pd.Series()).max()
        return self.holding.loc[lambda df: df['trade_dt'] == max_dt]


class FastBackTester(metaclass=abc.ABCMeta):
    asset = AssetEnum.STOCK
    deal_price = 'close'

    def __init__(self):
        self.accounts = dict()
        self.pre_dt = None
        self.cur_dt = None

    def run(self, start_date, end_date, freq):
        self.accounts.clear()
        orders = defaultdict(dict)

        for dt in get_dates(freq):
            self.pre_dt, self.cur_dt = self.cur_dt, dt
            price_board = self.get_bar_data(self.cur_dt)

            for name, account in self.accounts:
                account: FastAccount
                # if account.holding.empty:
                #     a
                # account.market_value[self.cur_dt]

            for acc_name, acc_orders in orders.items():
                account = self.accounts.setdefault(
                    acc_name,
                    FastAccount(market_value={self.cur_dt: 1e4}, cash={self.cur_dt: 1e4}, holding=pd.DataFrame())
                )

    @abc.abstractmethod
    def handle_bar(self):
        return NotImplementedError

    def get_bar_data(self, dt):
        # If need local data, change this.
        price_data = get_price(self.asset, dt, dt).rename(
            columns={'adj_nav': self.deal_price}
        ).set_index('wind_code')
        return price_data[self.deal_price].mul(price_data.get('adj_factor', 1), fill_value=1)
