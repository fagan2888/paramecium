# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:21
@Author: Sue Zhu
"""
from functools import wraps


def df2records(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        raw_data = func(*args, **kwargs)
        return [record.dropna().to_dict() for _, record in raw_data.iterrows()]

    return wrapper
