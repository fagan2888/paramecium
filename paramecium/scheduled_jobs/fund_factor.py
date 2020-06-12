# -*- coding: utf-8 -*-
"""
@Time: 2020/6/10 12:43
@Author: Sue Zhu
"""
import importlib

from ._base import *
from .. import const
from ..database import FactorDBTool
from ..database.pg_models import monitors


class FundFactorUpdate(BaseJob):
    """
    基金因子更新列表
    """

    @staticmethod
    def factory(factor_cls, **params):
        return factor_cls(**params)

    def run(self, *args, **kwargs):
        with get_session() as ss:
            factor_list = ss.query(monitors.FundFactorList).filter(monitors.FundFactorList.status == 1).all()

        for instance in factor_list:
            module_path, cls = instance.module_path.rsplit('.', maxsplit=1)
            factor = getattr(importlib.import_module(module_path), cls)(**instance.params)
            io_ = FactorDBTool(factor)
            io_.localized_time_series(io_.get_max_date(), freq=const.FreqEnum[instance.calc_freq])
