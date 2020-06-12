# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 9:44
@Author: Sue Zhu
"""
from . import const
from .database import FactorDBTool
from .interface import AbstractFactor


def generate_analysis_data(factor: AbstractFactor, start_date, end_date=None, freq=const.FreqEnum.M):
    """
    获取因子分析用数据
    :param factor: Factor对象，记录截面因子计算逻辑
    :param start_date: 开始日期
    :param end_date: 结束日期
    :param freq: 频率
    :param io_:
    :return:
    """
    factor_io = FactorDBTool(factor)
