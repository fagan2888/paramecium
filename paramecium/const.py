# -*- coding: utf-8 -*-
"""
@Time: 2020/5/27 16:12
@Author: Sue Zhu
"""
__all__ = ['AssetEnum', 'FreqEnum', 'TradeStatus']

from enum import Enum


class CustomEnum(Enum):
    @classmethod
    def items(cls):
        return {**{n: v.value for n, v in cls.__members__.items()}}


class AssetEnum(CustomEnum):
    STOCK = 'stock'
    CMF = 'mf'
    INDEX = 'index'


class FreqEnum(CustomEnum):
    D = 250
    W = 50
    M = 12
    Q = 4
    Y = 1


class TradeStatus(CustomEnum):
    """ Bar交易状态 """
    CHECK = -2  # 待核查
    TRADE = -1  # 交易
    SUSPEND = 0  # 停牌
    XR = 1  # 除权
    XD = 2  # 除息
    DR = 3  # 除权除息
    N = 4  # 上市首日
