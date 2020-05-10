# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 14:40
@Author:  MUYUE1
"""
import json
import logging
import re

import numpy as np
import pandas as pd

from paramecium.database import model_stock_org
from simple_scheduler.jobs._crawler import WebCrawlerJob, TushareCrawlerJob

logger = logging.getLogger(__name__)


class AShareDescription(TushareCrawlerJob):
    """
    Crawler stock description from tushare
    """
    _COL = {
        'ts_code': 'wind_code',
        'name': 'short_name',
        'comp_name': 'comp_name',
        'comp_name_en': 'comp_name_en',
        'isin_code': 'isin_code',
        'exchange': 'exchange',
        'list_board': 'list_board',
        'list_date': 'list_dt',
        'delist_date': 'delist_dt',
        'crncy_code': 'currency',
        'pinyin': 'pinyin',
        'is_shsc': 'is_shsc',
        'comp_code': 'comp_code',
    }

    @classmethod
    def meta_info(cls):
        return {
            **super().meta_info(),
            'arguments': [],
            'example_arguments': ''
        }

    @property
    def model(self):
        return model_stock_org.AShareDescription

    def run(self, *args, **kwargs):
        stock_info = pd.concat((
            self.api.stock_basic(
                exchange=exchange, fields=','.join(self._COL.keys())
            ) for exchange in ('SSE', 'SZSE')
        )).rename(columns=self._COL).fillna(np.nan)
        stock_info.loc[:, 'list_dt'] = pd.to_datetime(stock_info['list_dt'])
        stock_info.loc[:, 'delist_dt'] = pd.to_datetime(stock_info['delist_dt']).fillna(pd.Timestamp.max)
        self.upsert_data(
            records=(record.dropna() for _, record in stock_info.iterrows()),#.to_dict(orient='records')
            ukeys=[self.model.wind_code]
        )


class AShareAnnouncement(WebCrawlerJob):
    """
    Crawler stock announcement from `eastmoney.com`
    """

    @classmethod
    def meta_info(cls):
        return {
            **super().meta_info(),
            'arguments': [
                # is_update
                {'type': 'int', 'description': '1: update mode, 0: replace mode'},
                # argument2
                {'type': 'string', 'description': 'Second argument'}
            ],
            'example_arguments': '[1, ]'
        }

    def __init__(self, job_id, execution_id):
        super().__init__(job_id, execution_id)
        self.security = {}
        self.board = {}
        self.security = {}

    @staticmethod
    def url(var_name, dt, page=1):
        return (f'http://data.eastmoney.com/notices/getdata.ashx?StockCode=&FirstNodeType=0&CodeType=1&'
                f'PageIndex={page}&PageSize=100&jsObj={var_name}&SecNodeType=0&Time={dt:%Y-%m-%d}')

    @staticmethod
    def format_data(raw_dict):
        security = raw_dict['CDSY_SECUCODES']
        return pd.Series({
            'affect_dt': pd.Timestamp(raw_dict['ENDDATE']).date(),
            'ann_dt': pd.Timestamp(raw_dict['NOTICEDATE']).date(),
            'title': raw_dict['NOTICETITLE'],

            # "http://data.eastmoney.com/notices/detail/{symbol}/{info_code},{random_code}.html"
            'em_info_code': raw_dict['INFOCODE'],
            'em_table_id': raw_dict['TABLEID'],

            'symbol': raw_dict['CDSY_SECUCODES']['SECURITYCODE'],
            'short_name': raw_dict['CDSY_SECUCODES']['SECURITYSHORTNAME'],
        })

    def run(self, start_date='', *args, **kwargs):
        if start_date:
            start_date = pd.Timestamp(start_date)
        else:
            start_date = pd.Timestamp.now() - pd.Timedelta(days=1)

        for dt in pd.date_range(start_date, pd.Timestamp.now()):
            var_name = self.random_str(8)
            respond = self.comment_respond('GET', self.url(var_name, dt))
            if respond.status_code == 200:
                respond_data = json.loads(respond.text.replace(f'var {var_name} = ', '', count=1)[:-1])


if __name__ == '__main__':
    from uuid import uuid4

    AShareDescription(uuid4(), uuid4()).run()
