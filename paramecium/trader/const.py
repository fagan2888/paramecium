# -*- coding:utf-8 -*-
"""
Created at 2019/7/7 by Sue.

Usage:
    
"""
from enum import Enum


class CustomEnum(Enum):

    def __iter__(self):
        for key, obj in self.__member__():
            yield key, obj.value


class AssetTypeEnum(CustomEnum):
    STOCK = 'a_share'
    MF = 'cn_mf'
    INDEX = 'index_'


class OrderType(CustomEnum):
    NEW = 'new'
    FILLED = 'filled'
    CANCELED = 'canceled'
    REJECTED = 'rejected'
