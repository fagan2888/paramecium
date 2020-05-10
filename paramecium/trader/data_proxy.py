# -*- coding:utf-8 -*-
"""
Created at 2019/7/6 by Sue.

Usage:
    
"""
import abc


class AbstractDataProxy(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_trade_dates(self, freq, exchange='SSE'):
        pass

    @abc.abstractmethod
    def get_date_offset(self, base, n, freq, backward=True, exchange='SSE'):
        pass

    @abc.abstractmethod
    def get_interest_rate(self, rate_type):
        pass

    @abc.abstractmethod
    def get_all_instruments(self, asset, sector_type):
        pass

    @abc.abstractmethod
    def get_history_price(self, asset, start, end, freq=None):
        pass

    @abc.abstractmethod
    def get_dividend(self, asset, dt):
        pass

    @abc.abstractmethod
    def get_split(self, asset, dt):
        pass

    # ---- 公募基金信息 -------------------------------------
    def get_fund_manager(self, dt, fund_id=None, manager_id=None):
        raise NotImplementedError

    def get_fund_portfolio(self, report_dt, portfolio_type, fund_id=None):
        raise NotImplementedError
