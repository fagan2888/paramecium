# -*- coding: utf-8 -*-
"""
@Time: 2020/6/10 12:43
@Author: Sue Zhu
"""
from ..factor_io import FactorDBTool
from ... import const
from ...factor_pool import fund_stats

from ._base import *


class FundFactorUpdate(BaseLocalizerJob):
    """
    基金因子更新列表
    """

    def run(self, *args, **kwargs):
        fund_list = [
            fund_stats.FundPerform(63, const.FreqEnum.D),
            fund_stats.FundRegFF3(63, const.FreqEnum.D),
            fund_stats.FundRegFF3(125, const.FreqEnum.D),
            fund_stats.FundRegFF3(250, const.FreqEnum.D),
            fund_stats.FundRegBond5(63, const.FreqEnum.D),
            fund_stats.FundRegBond5(125, const.FreqEnum.D),
            fund_stats.FundRegBond5(250, const.FreqEnum.D),
        ]
        for factor in fund_list:
            io_ = FactorDBTool(factor)
            io_.localized_time_series(factor.start_date, )
