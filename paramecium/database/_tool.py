# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:55
@Author: Sue Zhu
"""
import json
from pathlib import Path

REQUEST_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/81.0.4044.138 Safari/537.36',
}


def flat_1dim(folder_data):
    return (entry for record in folder_data for entry in record)


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


def get_tushare_col_mapping(ts_api):
    """ mapping between tushare and postgresql database """
    json_path = Path(__file__).parent.joinpath('tushare_mapping.json')
    with open(json_path, 'r', encoding='utf8') as load_f:
        mapping_dict = json.load(load_f).get(ts_api, {})
    mapping_dict.setdefault('ts_code', 'wind_code')
    return mapping_dict
