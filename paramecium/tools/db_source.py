# -*- coding: utf-8 -*-
"""
@Time: 2020/5/13 8:39
@Author: Sue Zhu
"""
import configparser
from functools import lru_cache
from pathlib import Path
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, scoped_session
from iFinDPy import THS_iFinDLogin, THS_iFinDLogout

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


def get_session_factory(env='postgres'):
    session_factory = sessionmaker(bind=get_sql_engine(env))
    Session = scoped_session(session_factory)
    return Session


@contextmanager
def get_ifind_api():
    err_code = THS_iFinDLogin(**get_data_config('ifind'))
    if err_code == '2':
        print("iFind API has an error!")
    else:
        print("Successful login IFind")
        yield err_code
    print("iFind API")
    THS_iFinDLogout()
