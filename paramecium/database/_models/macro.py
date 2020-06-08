# -*- coding: utf-8 -*-
"""
@Time: 2020/6/5 11:09
@Author: Sue Zhu
"""
from .utils import *


class InterestRate(BaseORM):
    """ 基准利率调整 """
    __tablename__ = 'macro_interest_rate'

    change_dt = sa.Column(sa.Date, primary_key=True)
    save_rate = sa.Column(pg.REAL)  # 单位：%
    loan_rate = sa.Column(pg.REAL)  # 单位：%
