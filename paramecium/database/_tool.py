# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:55
@Author: Sue Zhu
"""
import json
from pathlib import Path


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
