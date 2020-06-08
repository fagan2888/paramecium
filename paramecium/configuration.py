# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:53
@Author: Sue Zhu
"""
__all__ = ['get_data_config']

import configparser
from pathlib import Path


def get_data_config(section):
    """
    配置文件, 地址 `~/parameciums.conf`
    :param section: str
    :return: config object
    """
    config = configparser.ConfigParser()
    config.read(Path.home().joinpath('parameciums.conf'))
    return config[section]
