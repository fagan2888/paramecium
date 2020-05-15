# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 20:58
@Author: Sue Zhu
"""
import logging

import numpy as np
import pandas as pd
import sqlalchemy as sa

from paramecium.database import model_fund_org
from simple_scheduler.jobs._crawler import TushareCrawlerJob

logger = logging.getLogger(__name__)


class FundDescription(TushareCrawlerJob):
    """
    Crawler trade calendar from tushare
    """
    meta_args = (
        # pre_truncate:
        {'type': 'int', 'description': '0 or 1 as bool, default `1`'},
    )
    meta_args_example = '[1]'

    def run(self, pre_truncate=1, *args, **kwargs):
        model = model_fund_org.MutualFundDescription

        if pre_truncate:
            session = self.sa_session
            try:
                logger.info(f'create table {self.__class__.__name__:s}.')
                session.execute(f'truncate table {model.__tablename__};')
            except Exception as e:
                logger.warning(f'{self.__class__.__name__:s} fail to truncate table before insert '
                               f'with exception {repr(e)}.')
                session.rollback()
            else:
                session.commit()

        fund_info = pd.concat(
            (self.api.fund_basic(market=m) for m in list('OE')),
            axis=0, sort=False
        ).fillna(np.nan).rename(columns={
            'ts_code': 'wind_code',
            'name': 'short_name',
            'fund_type': 'invest_type',
            'found_date': 'setup_date',
            'due_date': 'maturity_date',
            'purc_startdate': 'purchase_start_dt',
            'redm_startdate': 'redemption_start_dt',
            'invest_type': 'invest_style',
            'type': 'fund_type',
        }).filter(model.__dict__.keys(), axis=1)

        for c in (
                'setup_date', 'maturity_date', 'issue_date',
                'list_date', 'delist_date',
                'purchase_start_dt', 'redemption_start_dt',
        ):
            fund_info.loc[:, c] = pd.to_datetime(fund_info[c])

        fund_info.loc[
            lambda df: df['setup_date'].notnull() & df['maturity_date'].isnull(), 'maturity_date'] = pd.Timestamp.max
        self.upsert_data(
            records=(record.dropna() for _, record in fund_info.iterrows()),
            model=model, ukeys=[model.wind_code]
        )


if __name__ == '__main__':
    FundDescription().run(True)
