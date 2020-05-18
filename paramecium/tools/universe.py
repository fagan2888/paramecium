# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 19:09
@Author: Sue Zhu
"""
import pandas as pd
import abc
from functools import lru_cache
from paramecium.database import model_fund_org as fund


class UniverseABC(object):
    """
    Universe Interface
    """

    def get_instruments(self, dt):
        return NotImplementedError


class FundUniverse(UniverseABC):

    def __init__(self, include_=None, exclude_=None, initial_only=True, no_grad=True,
                 open_only=True, issue_=12, size_=0.5):
        self.include = include_
        self.exclude = exclude_
        self.initial_only = initial_only
        self.no_grad = no_grad
        self.open_only = open_only
        self.issue = issue_ * 30  # simply think there is 30 days each month.
        self.size = size_

    @lru_cache(maxsize=2)
    def get_instruments(self, dt):
        filters = [
            fund.MutualFundDescription.setup_date <= dt - pd.Timedelta(days=self.issue)
        ]
        if self.include:
            filters.append(fund.MutualFundDescription.invest_type.in_(self.include))
        if self.exclude:
            filters.append(fund.MutualFundDescription.invest_type.notin_(self.exclude))

        if self.no_grad:
            filters.append(fund.MutualFundDescription.grad_type < 1)
        if self.open_only:
            filters.append(fund.MutualFundDescription.fund_type == '契约型开放式')


        # TODO: Size filter has not apply
