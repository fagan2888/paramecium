# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 9:50
@Author: Sue Zhu
"""
import logging
from contextlib import contextmanager
from functools import lru_cache

import numpy as np
import pandas as pd

from ..configuration import get_data_config

_log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_tushare_api(api_name='tushare_prod'):
    """
    获取Tushare API
    :param api_name: string
    """
    config = get_data_config(api_name)
    ts = __import__(config.pop('module_name'))
    ts_api = ts.pro_api(token=config['token'], env=config['env'])
    _log.info(f"Using TuShare API `{ts.__name__}`.")
    return ts_api


def get_tushare_data(api_name, date_cols=None, fields=None, col_mapping=None, env='prod', **func_kwargs):
    _log.debug(f"[{env}]{api_name}: {func_kwargs}")
    api = get_tushare_api(f'tushare_{env}')
    result = api.query(api_name, **func_kwargs, fields=','.join(fields) if fields else '').fillna(np.nan)
    if date_cols:
        for c in date_cols:
            result.loc[:, c] = pd.to_datetime(result[c])
    if col_mapping:
        result = result.rename(columns=col_mapping)
    return result


@contextmanager
def get_ifind_api():
    """ 获取同花顺IFind API """
    from iFinDPy import THS_iFinDLogin, THS_iFinDLogout
    err_code = THS_iFinDLogin(**get_data_config('ifind'))
    if err_code == '2':
        _log.error("iFind API has an error!")
    else:
        _log.debug("Successful login IFind")
        yield err_code
    _log.debug("Try to logout IFind")
    THS_iFinDLogout()


@contextmanager
def get_wind_api():
    """ 登录Wind API """
    from WindPy import w
    w.start()  # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒
    if w.isconnected():  # 判断WindPy是否已经登录成功
        yield w
    else:
        _log.error("Wind API fail to be connected.")
    w.stop()


class WindDataError(Exception):
    pass