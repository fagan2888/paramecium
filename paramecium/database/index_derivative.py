# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 13:55
@Author: Sue Zhu
"""
from .utils import *


class IndexDerivativeDesc(BaseORM):
    __tablename__ = 'index_derivative_description'

    benchmark_code = sa.Column(sa.String(40), primary_key=True)
    benchmark_name = sa.Column(sa.String(40))
    base_date = sa.Column(sa.Date)
    base_point = sa.Column(sa.Float)


class IndexDerivativePrice(BaseORM):
    __tablename__ = 'index_derivative_price'

    oid = gen_oid()
    benchmark_code = sa.Column(sa.String(40), index=True)
    trade_dt = sa.Column(sa.Date, index=True)
    close_ = sa.Column(sa.Float)
