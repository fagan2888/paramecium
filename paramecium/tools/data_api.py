# -*- coding: utf-8 -*-
"""
@Time: 2020/5/13 8:39
@Author: Sue Zhu
"""
import logging
import configparser
from functools import lru_cache
from pathlib import Path
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, scoped_session

_log = logging.getLogger(__name__)


def get_data_config(section):
    """
    获取配置文件, 配置文件地址 `~/parameciums.conf`
    :param section: str
    :return: config object
    """
    config = configparser.ConfigParser()
    config.read(Path.home().joinpath('parameciums.conf'))
    return config[section]


@lru_cache(maxsize=1)
def get_tushare_api(api_name='tushare_prod'):
    """
    获取Tushare API
    :param api_name:
    :return:
    """
    config = get_data_config(api_name)
    ts = __import__(config['module_name'])
    return ts.pro_api(token=config['token'], env=config['env'])


def get_sql_engine(env='postgres', **kwargs):
    params = dict(pool_size=30, encoding='utf-8')
    params.update(**kwargs)
    return create_engine(URL(**get_data_config(env)), **params)


@contextmanager
def get_ifind_api():
    from iFinDPy import THS_iFinDLogin, THS_iFinDLogout
    err_code = THS_iFinDLogin(**get_data_config('ifind'))
    if err_code == '2':
        _log.error("iFind API has an error!")
    else:
        _log.debug("Successful login IFind")
        yield err_code
    _log.debug("Try to logout IFind")
    THS_iFinDLogout()
