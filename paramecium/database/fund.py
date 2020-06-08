# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:52
@Author: Sue Zhu
"""
from functools import lru_cache

from ..interface import AbstractUniverse


@lru_cache()
class FundUniverse(AbstractUniverse):

    def __init__(self, include_=None, exclude_=None, initial_only=True, no_grad=True,
                 open_only=True, issue_month=12, size_=0.5):
        self.include = include_
        self.exclude = exclude_
        self.initial_only = initial_only
        self.no_grad = no_grad
        self.open_only = open_only
        self.issue = issue_month * 30  # simply think there is 30 days each month.
        self.size = size_

    @lru_cache(maxsize=2)
    def get_instruments(self, dt):
        # filters = [model_fund_org.MutualFundDescription.setup_date <= dt - pd.Timedelta(days=self.issue)]
        # if self.include:
        #     filters.append(model_fund_org.MutualFundDescription.invest_type.in_(self.include))
        # if self.exclude:
        #     filters.append(model_fund_org.MutualFundDescription.invest_type.notin_(self.exclude))
        #
        # if self.no_grad:
        #     filters.append(model_fund_org.MutualFundDescription.grad_type < 1)
        # if self.open_only:
        #     filters.append(model_fund_org.MutualFundDescription.fund_type == '契约型开放式')
        #
        # TODO: Size filter has not apply
        pass