# -*- coding: utf-8 -*-
"""
@Time: 2020/5/13 8:39
@Author: Sue Zhu
"""
import configparser
from functools import lru_cache
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, scoped_session


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
def get_tushare_api(api_name='tushare_prd'):
    """
    获取Tushare API
    :param api_name:
    :return:
    """
    config = get_data_config(api_name)
    ts = __import__(config['module_name'])
    return ts.pro_api(token=config['token'], env=config['env'])


def get_sql_engine(env='postgres'):
    return create_engine(URL(**get_data_config(env)), pool_size=30, encoding='utf-8'),# echo=True


def _get_session_factor(env='postgres'):
    session_factory = sessionmaker(bind=get_sql_engine(env))
    Session = scoped_session(session_factory)
    return Session
