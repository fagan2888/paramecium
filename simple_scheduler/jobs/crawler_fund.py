# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 20:58
@Author: Sue Zhu
"""
import logging

import numpy as np
import pandas as pd

from paramecium.database import model_fund_org
from simple_scheduler.jobs._crawler import TushareCrawlerJob

logger = logging.getLogger(__name__)


class FundDescription(TushareCrawlerJob):
    """
    Crawler trade calendar from tushare
    """

    @classmethod
    def meta_info(cls):
        return {
            **super().meta_info(),
            'arguments': [
                # pre_truncate:
                {'type': 'bool', 'description': 'default `True`'},
            ],
            'example_arguments': '[True, ]'
        }

    @property
    def model(self):
        return model_fund_org.MutualFundDescription

    def run(self, pre_truncate=True, *args, **kwargs):
        if pre_truncate:
            session = self.sa_session
            try:
                session.execute(f'truncate table {self.model.__tablename__};')
            except Exception as e:
                logger.warning(f'{self.__class__.__name__:s} fail to truncate table before insert '
                               f'with exception {repr(e)}.')
                session.rollback()
            else:
                session.commit()
        find_data = pd.read_excel(
            'F:\\PiggyLab\\paramecium\\others\\fund_list_ifind.xlsx',
            names=['wind_code', 'short_name', 'invest_type', 'is_index', 'grad_type', 'fund_type',
                   'full_name', 'setup_date', 'maturity_date'],
            parse_dates=['setup_date', 'maturity_date']
        ).replace('--', np.nan).dropna(subset=['short_name'])
        find_data.loc[:, 'invest_type'] = find_data['invest_type'].map({
            '普通股票型基金': '20010101010000',
            '被动指数型股票基金': '20010101020000',
            '增强指数型股票基金': '20010101030000',
            '偏股混合型基金': '20010102010000',
            '股债平衡型基金': '20010102020000',
            '偏债混合型基金': '20010102030000',
            '灵活配置型基金': '20010102040000',
            '中长期纯债券型基金': '20010103010000',
            '短期纯债券型基金': '20010103020000',
            '混合债券型基金(一级)': '20010103030000',
            '混合债券型基金(二级)': '20010103040000',
            '被动指数型债券基金': '20010103050000',
            '增强指数型债券基金': '20010103060000',
            '货币市场基金': '20010104000000',
            '保本基金': '20010105020000',
            '混合型FOF': '20010105040000',
            '股票型FOF': '20010105040000',
            '商品型基金': '20010105050000',
            'QDII基金': '20010108000000',
        })
        find_data.loc[:, 'is_index'] = find_data['is_index'].eq('是').astype(int)
        find_data.loc[:, 'grad_type'] = find_data['grad_type'].map({
            '分级母基金': 0,
            '分级A类基金': 1,
            '分级B类基金': 2,
            np.nan: -1
        }).astype(int)
        find_data.loc[
            lambda df: df['setup_date'].notnull() & df['maturity_date'].isnull(), 'maturity_date'] = pd.Timestamp.max
        self.upsert_data(
            records=(record.dropna() for _, record in find_data.iterrows()),
            model=self.model,
            ukeys=[self.model.wind_code]
        )


if __name__ == '__main__':
    FundDescription().run(True)