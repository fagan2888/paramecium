# -*- coding: utf-8 -*-
"""
@Time: 2020/5/10 17:55
@Author: Sue Zhu
"""
from ._base import *


class EnumIndustryCode(BaseORM):
    __tablename__ = 'enum_industry_code'

    industry_code = sa.Column(sa.String(40), primary_key=True)
    industry_name = sa.Column(sa.String(40))
    level_num = sa.Column(sa.Integer)
    memo = sa.Column(sa.String)


