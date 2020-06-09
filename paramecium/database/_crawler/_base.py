# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 23:32
@Author: Sue Zhu
"""
__all__ = [
    'BaseLocalizerJob', 'TushareCrawlerJob', 'WebCrawlerJob',
    'get_wind_api', 'WindDataError',
    'get_session', 'clean_duplicates'
]

from requests import request

from ..scheduler import BaseLocalizerJob
from .._third_party_api import get_tushare_data, get_wind_api, WindDataError
from .._tool import get_tushare_col_mapping, REQUEST_HEADER
from .._postgres import get_session, clean_duplicates


class TushareCrawlerJob(BaseLocalizerJob):

    def run(self, env='prod', *args, **kwargs):
        self.env = env

    def get_tushare_data(self, api_name, date_cols=None, fields=None, **func_kwargs):
        mapping = get_tushare_col_mapping(api_name)
        if fields:
            fields = {*fields, *mapping.keys()}
        return get_tushare_data(
            api_name=api_name, date_cols=date_cols, fields=fields,
            col_mapping=get_tushare_col_mapping(api_name), env=self.env, **func_kwargs
        )


class WebCrawlerJob(BaseLocalizerJob):

    def request(self, url, method='GET'):
        responds_raw = request(method=method, url=url, headers=REQUEST_HEADER)
        if responds_raw.status_code == 200:
            return responds_raw
        else:
            self.get_logger().error(f'Fail to get respond data with code {responds_raw.status_code}')
