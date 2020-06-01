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



