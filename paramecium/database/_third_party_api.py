# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 9:50
@Author: Sue Zhu
"""
import json
import logging
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import typing

from ..configuration import get_data_config

_log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _tushare_api(api_name='tushare_prod'):
    """
    获取Tushare API
    :param api_name: string
    """
    config = get_data_config(api_name)
    ts = __import__(config.pop('module_name'))
    ts_api = ts.pro_api(**config)
    _log.info(f"Using TuShare API `{ts.__name__}`.")
    return ts_api


def get_tushare_data(api_name, fields=None, env='prod', **func_kwargs):
    """
    获取Tushare数据，并做一些基本处理，例如rename column, recognize timestamp
    """
    _log.debug(f"[{env}]{api_name}: {func_kwargs}")
    api = _tushare_api(f'tushare_{env}')

    # mapping between tushare and postgresql database
    json_path = Path(__file__).parent.joinpath('tushare_mapping.json')
    with open(json_path, 'r', encoding='utf8') as load_f:
        conf = json.load(load_f).get(api_name, {})

    col_mapping = conf.get('mapping', {})
    col_mapping.setdefault('ts_code', 'wind_code')

    # mapping fields to old one.
    inv_col = {v: k for k, v in col_mapping.items()}
    if fields:
        fields_str = ','.join((inv_col.get(k, k) for k in fields))
    else:
        fields_str = ''

    # query ts api
    result = api.query(api_name, **func_kwargs, fields=fields_str).fillna(np.nan)
    result = result.rename(columns=col_mapping)

    # transform dt cols
    for c in conf.get('dt_col', []):
        result.loc[:, c] = pd.to_datetime(result[c], format='%Y%m%d')

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
    w.start()  # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数
    if w.isconnected():  # 判断WindPy是否已经登录成功
        yield w
    else:
        _log.error("Wind API fail to be connected.")
    w.stop()


def get_wind_conf(key):
    json_path = Path(__file__).parent.joinpath('wind_api_conf.json')
    with open(json_path, 'r', encoding='utf8') as load_f:
        conf = json.load(load_f)[key]
    return conf


class WindDataError(Exception):
    pass
