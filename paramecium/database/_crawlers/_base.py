# -*- coding: utf-8 -*-
"""
@Time: 2020/6/12 13:45
@Author: Sue Zhu
"""
__all__ = ['CrawlerJob', 'get_wind_conf', 'get_session', 'get_type_codes']

import numpy as np
import pandas as pd
from WindPy import w
from requests import request

from .._postgres import get_session
from .._third_party_api import get_tushare_data, WindDataError, get_wind_conf
from .._tool import get_type_codes
from ..scheduler import BaseJob

w.start()  # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数
if not w.isconnected():  # 判断WindPy是否已经登录成功
    raise WindDataError("Wind API fail to be connected.")


class CrawlerJob(BaseJob):
    TS_ENV = 'prod'

    def get_tushare_data(self, api_name, fields=None, **func_kwargs):
        return get_tushare_data(api_name=api_name, fields=fields, env=self.TS_ENV, **func_kwargs)

    def request_from_web(self, url, method='GET'):
        responds_raw = request(method=method, url=url, headers=REQUEST_HEADER)
        if responds_raw.status_code == 200:
            return responds_raw
        else:
            self.get_logger().error(f'Fail to get respond data with code {responds_raw.status_code}')

    @staticmethod
    def query_wind(api_name, col_mapping=None, dt_cols=None, col='Fields', index='Times', **func_kwargs):
        if 'fields' in func_kwargs.keys():
            func_kwargs['fields'] = ','.join(func_kwargs['fields']).lower()

        api = getattr(w, api_name)
        error, data = api(**func_kwargs, usedf=True)
        if error:
            raise WindDataError(f'Wind Data Api Error with {error}')
        else:
            data = data.fillna(np.nan)
            if col_mapping:
                data = data.rename(columns=col_mapping)
            else:
                data = data.rename(columns=str.lower)
            for col in (dt_cols if dt_cols else []):
                data.loc[:, col] = pd.to_datetime(data[col].where(lambda ser: ser > '1899-12-30', pd.Timestamp.max))
            return data


REQUEST_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/81.0.4044.138 Safari/537.36',
}
