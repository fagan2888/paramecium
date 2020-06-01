# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 10:50
@Author: Sue Zhu
"""
import pandas as pd
from paramecium.database.factor_io import FactorDBTool
from paramecium.factor_pool.stock_classic import FamaFrench
from paramecium.scheduler_.jobs._localizer import BaseLocalizerJob


class StockFF3Factor(BaseLocalizerJob):
    """
    Localized Stock Fama-French Data
    """
    meta_args = (
        {'type': 'string', 'description': 'date string that can be translate by pd.to_datetime'}
    )

    def run(self, start=None, end=None, *args, **kwargs):
        ff3_ = FamaFrench()
        io_ = FactorDBTool(ff3_)

        io_.get_latest_date()
        io_.localized_time_series(start, end)


