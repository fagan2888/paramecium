# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:52
@Author: Sue Zhu
"""
from functools import lru_cache

import numpy as np
import pandas as pd
import sqlalchemy as sa
from pandas.tseries.offsets import QuarterEnd

from ._postgres import get_session
from ._tool import get_type_codes
from .comment import get_sector, get_dates
from .pg_models import fund
from .. import const
from ..interface import AbstractUniverse


@lru_cache(maxsize=4)
class FundUniverse(AbstractUniverse):

    def __init__(self, include_=(),
                 # 定期开放,委外,机构,可转债
                 exclude_=("1000007793000000", "1000027426000000", "1000031885000000", "1000023509000000"),
                 initial_only=True, open_only=True, issue=250, size_=0.5, manager=250):
        self.include = include_
        self.exclude = exclude_
        self.initial_only = initial_only
        self.open_only = open_only
        self.issue = issue  # trade days
        self.size = size_
        self.manager = manager  # TODO

    def __str__(self):
        mapping = get_type_codes('mf_org_sector_m')['sector_code']
        include = ','.join(map(lambda x: mapping.get(x, x), self.include))
        exclude = ','.join(map(lambda x: mapping.get(x, x), self.exclude))
        return (f"Fund(include=[{include}], exclude=[{exclude}], initial={self.initial_only}, open={self.open_only}, "
                f"issue={self.issue}days), size={self.size}*1e8, manager={self.manager}days")

    @lru_cache(maxsize=2)
    def get_instruments(self, month_end):
        issue_dt = [t for t in get_dates(const.FreqEnum.D) if t < month_end][-self.issue]
        with get_session() as ss:
            filters = [
                # date
                fund.Description.setup_date <= issue_dt,
                fund.Description.redemption_start_dt <= month_end,
                fund.Description.maturity_date >= month_end,
                # not connect fund
                fund.Description.wind_code.notin_(ss.query(fund.Connections.child_code)),
                # issue reset after convert happened.
                sa.not_(ss.query(fund.Converted.wind_code).filter(
                    fund.Converted.chg_date > issue_dt,
                    fund.Converted.chg_date <= month_end,
                    fund.Converted.wind_code == fund.Description.wind_code
                ).exists())
            ]
            if self.open_only:
                filters.append(fund.Description.fund_type == '契约型开放式')
            if self.initial_only:
                filters.append(fund.Description.is_initial == 1)
            if self.size > 0:
                filters.append(ss.query(fund.PortfolioAsset.wind_code).filter(
                    fund.PortfolioAsset.net_asset >= self.size * 1e8,
                    fund.PortfolioAsset.end_date == (month_end - pd.Timedelta(days=22) - QuarterEnd()),
                    fund.PortfolioAsset.wind_code == fund.Description.wind_code
                ).exists())
            # if self.manager:
            #
            fund_list = {code for (code,) in ss.query(fund.Description.wind_code).filter(*filters).all()}

        if self.include or self.exclude:
            sector_type = pd.concat((
                get_sector(const.AssetEnum.CMF, valid_dt=month_end, sector_prefix='2001'),
                get_sector(
                    const.AssetEnum.CMF, sector_prefix='1000',
                    valid_dt=max((t for t in get_dates(const.FreqEnum.Q) if t < month_end)),
                ),
            ))
            if self.include:
                in_fund = sector_type.loc[lambda df: df['sector_code'].isin(self.include), 'wind_code']
                fund_list = fund_list & {*in_fund}
            if self.exclude:
                ex_fund = sector_type.loc[lambda df: df['sector_code'].isin(self.exclude), 'wind_code']
                fund_list = fund_list - {*ex_fund}

        return fund_list


def get_convert_fund(valid_dt):
    with get_session() as ss:
        query = pd.DataFrame(
            ss.query(
                fund.Converted.wind_code,
                fund.Converted.chg_date,
                fund.Converted.ann_date,
                fund.Converted.memo
            ).filter(fund.Converted.chg_date <= valid_dt).all()
        ).set_index('wind_code').fillna(np.nan)
    return query
