# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:52
@Author: Sue Zhu
"""
from functools import lru_cache
import pandas as pd

from .comment import get_sector
from ._models import fund
from ..const import AssetEnum
from ..interface import AbstractUniverse


@lru_cache()
class FundUniverse(AbstractUniverse):

    def __init__(
            self, include_=None,
            # 定期开放,委外,机构,可转债
            exclude_=("1000007793000000", "1000027426000000", "1000031885000000", "1000023509000000"),
            initial_only=True, no_grad=True,
            open_only=True, issue_month=12, size_=0.5
    ):
        self.include = include_
        self.exclude = exclude_
        self.initial_only = initial_only
        self.no_grad = no_grad
        self.open_only = open_only
        self.issue = issue_month * 30  # simply think there is 30 days each month.
        self.size = size_

    @lru_cache(maxsize=2)
    def get_instruments(self, month_end):
        filters = [fund.Description.setup_date <= month_end - pd.Timedelta(days=self.issue)]

        if self.include:
            asset_type = get_sector(AssetEnum.CMF, valid_dt=month_end, sector_prefix='2001')
            in_fund = asset_type.loc[lambda df: df['sector_code'].isin(self.include), 'wind_code']
        if self.exclude:
            asset_type = get_sector(AssetEnum.CMF, valid_dt=month_end, sector_prefix='1000')
            ex_fund = asset_type.loc[lambda df: df['sector_code'].isin(self.include), 'wind_code']

        if self.no_grad:
            filters.append(fund.Description.grad_type < 1)
        if self.open_only:
            filters.append(fund.Description.fund_type == '契约型开放式')

        # TODO: Size filter has not apply
        pass
