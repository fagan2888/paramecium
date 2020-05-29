# -*- coding: utf-8 -*-
"""
@Time: 2020/5/13 8:39
@Author: Sue Zhu
"""
import logging
from contextlib import contextmanager
from functools import lru_cache

from paramecium.utils.configuration import get_data_config

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
