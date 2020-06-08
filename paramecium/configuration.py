# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:53
@Author: Sue Zhu
"""
__all__ = ['get_data_config', 'get_type_codes']

import configparser
from pathlib import Path
import json


def get_data_config(section):
    """
    配置文件, 地址 `~/parameciums.conf`
    :param section: str
    :return: config object
    """
    config = configparser.ConfigParser()
    config.read(Path.home().joinpath('parameciums.conf'))
    return config[section]


def get_type_codes(table, field=None):
    """
    Type code in Wind database.
    Use local file instead of db table to make sync easier.
    :param table: str
    :param field: str
    :return: dict
    """
    json_path = Path(__file__).parent.joinpath('type_code.json')
    with open(json_path, 'r', encoding='utf8') as load_f:
        load_dict = json.load(load_f)[table]

    if field:
        return load_dict[field]
    else:
        return load_dict
