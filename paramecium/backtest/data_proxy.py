# -*- coding: utf-8 -*-
"""
@Time: 2020/4/20 22:23
@Author: Sue Zhu
"""


class DataProxy(object):

    def get_trading_dates(self, freq='D'):
        pass

    def get_date_offset(self, base, freq='D', n=0, backward=True):
        pass

    def get_all_instrument(self, asset, valid_dt, fields=None):
        pass

    def get_instrument(self, asset, wind_code):
        pass

    def get_latest_price(self, asset, end, adjust=False):
        pass

    def get_history_price(self, asset, start, end, freq, adjust=False):
        pass

    def get_sector(self, asset, sector_type, valid_dt, wind_code):
        pass


