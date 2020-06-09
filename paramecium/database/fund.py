# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:52
@Author: Sue Zhu
"""
from functools import lru_cache

import pandas as pd

from ._models import fund
from ._postgres import get_session
from .comment import get_sector, get_dates
from .. import const
from ..interface import AbstractUniverse


@lru_cache()
class FundUniverse(AbstractUniverse):

    def __init__(
            self, include_=None,
            # 定期开放,委外,机构,可转债
            exclude_=("1000007793000000", "1000027426000000", "1000031885000000", "1000023509000000"),
            initial_only=True, open_only=True, issue_month=12, size_=0.5
    ):
        self.include = include_
        self.exclude = exclude_
        self.initial_only = initial_only
        self.open_only = open_only
        self.issue = issue_month * 30  # simply think there is 30 days each month.
        self.size = size_  # TODO: size limit has not been apply.

    @lru_cache(maxsize=2)
    def get_instruments(self, month_end):
        quarter_end = max((t for t in get_dates(const.FreqEnum.Q) if t <= month_end))
        with get_session() as ss:
            filters = [
                # issue over month
                fund.Description.setup_date <= month_end - pd.Timedelta(days=self.issue),
                # not connect fund
                fund.Description.wind_code.notin_(ss.query(fund.Connections.child_code))
            ]
            if self.open_only:
                filters.append(fund.Description.fund_type == '契约型开放式')
            if self.initial_only:
                filters.append(fund.Description.is_initial == 1)

            fund_list = {code for (code,) in ss.query(fund.Description.wind_code).filter(*filters).all()}

        if self.include or self.exclude:
            sector_type = pd.concat((
                get_sector(const.AssetEnum.CMF, valid_dt=month_end, sector_prefix='2001'),
                get_sector(const.AssetEnum.CMF, valid_dt=quarter_end, sector_prefix='1000'),
            ))
            if self.include:
                in_fund = sector_type.loc[lambda df: df['sector_code'].isin(self.include), 'wind_code']
                fund_list = fund_list & {*in_fund}
            if self.exclude:
                ex_fund = sector_type.loc[lambda df: df['sector_code'].isin(self.exclude), 'wind_code']
                fund_list = fund_list - {*ex_fund}

        return fund_list
