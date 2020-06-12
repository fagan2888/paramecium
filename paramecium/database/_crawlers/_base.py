# -*- coding: utf-8 -*-
"""
@Time: 2020/6/12 13:45
@Author: Sue Zhu
"""
import numpy as np
import pandas as pd
from WindPy import w
from requests import request

from .._third_party_api import get_tushare_data, WindDataError
from .._tool import get_tushare_col_mapping, REQUEST_HEADER
from ..scheduler import BaseJob

w.start()  # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数
if not w.isconnected():  # 判断WindPy是否已经登录成功
    raise WindDataError("Wind API fail to be connected.")


class CrawlerJob(BaseJob):
    TS_ENV = 'prod'

    def get_tushare_data(self, api_name, date_cols=None, fields=None, **func_kwargs):
        mapping = get_tushare_col_mapping(api_name)
        if fields:
            fields = {*fields, *mapping.keys()}
        return get_tushare_data(
            api_name=api_name, date_cols=date_cols, fields=fields,
            col_mapping=get_tushare_col_mapping(api_name), env=self.TS_ENV, **func_kwargs
        )

    def request_from_web(self, url, method='GET'):
        responds_raw = request(method=method, url=url, headers=REQUEST_HEADER)
        if responds_raw.status_code == 200:
            return responds_raw
        else:
            self.get_logger().error(f'Fail to get respond data with code {responds_raw.status_code}')

    def query_wind(self, api_name, col_mapping=None, date_cols=None, **func_kwargs):
        # with get_wind_api() as w:
        api = getattr(w, api_name)
        error, data = api(**func_kwargs, usedf=True)
        if error:
            raise WindDataError(f'Wind Data Api Error with {error}')
        else:
            data = data.fillna(np.nan)
            if col_mapping:
                data = data.rename(columns=col_mapping)
            for col in (date_cols if date_cols else []):
                data.loc[:, col] = pd.to_datetime(data[col].where(lambda ser: ser > '1899-12-30', pd.Timestamp.max))
            return data
