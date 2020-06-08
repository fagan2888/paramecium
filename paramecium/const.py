# -*- coding: utf-8 -*-
"""
@Time: 2020/5/27 16:12
@Author: Sue Zhu
"""
__all__ = ['AssetEnum', 'FreqEnum', 'TradeStatus', 'SectorEnum']

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


class SectorEnum(CustomEnum):
    """ Sector 类型: 开始字母 """

    # Stock
    SEC_SW = '61'  # 申万行业分类
    SEC_WIND = '62'  # 万得全球行业分类标准
    SEC_SEC_OLD = '04'  # 证监会行业分类
    SEC_SEC = '12'  # 证监会行业分类(2012版)
    SEC_GICS = '67'  # GICS(全球行业分类标准)
    SEC_ZZ_OLD = '66'  # 中证行业分类
    SEC_ZZ = '72'  # 中证行业分类(2016)
    SEC_CITICS = 'b1'  # 中信行业分类

    SEC_REGIONAL = '03'  # 地域板块
    SEC_CONCEPT = '02'  # 概念板块

    # Mutual Fund
    MF_TYPE = '200101'
    MF_YH = '2003'  # 银河基金分类

    BOND = '0808'  # 中债分类


class WindSector(CustomEnum):
    """ Wind API wset 板块id """

    MF = '1000008492000000'
    MF_GRAD_PAR = '1000006545000000'
    MF_GRAD_A = '1000006546000000'
    MF_GRAD_B = '1000006547000000'
    ETF = 'a201010b00000000'
    MF_FIX_OPEN = "1000007793000000"  # 定期开放
    MF_TRUST = "1000027426000000"  # 委外
    MF_INS = "1000031885000000"  # 机构
    MF_CONVERT = "1000023509000000"  # 可转债
