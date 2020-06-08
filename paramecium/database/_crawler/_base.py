# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 23:32
@Author: Sue Zhu
"""

from ..scheduler import BaseLocalizerJob
from .._third_party_api import get_tushare_data
from .._tool import get_tushare_col_mapping


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


