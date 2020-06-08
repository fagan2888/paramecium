# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:55
@Author: Sue Zhu
"""


def flat_1dim(folder_data):
    return (entry for record in folder_data for entry in record)