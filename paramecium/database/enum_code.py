# -*- coding: utf-8 -*-
"""
@Time: 2020/5/10 17:55
@Author: Sue Zhu
"""
from .utils import *


class EnumIndustryCode(BaseORM):
    __tablename__ = 'enum_industry_code'

    code = sa.Column(name='industry_code', type_=sa.String(40), primary_key=True)
    name = sa.Column(name='industry_name', type_=sa.String(40))
    level_num = sa.Column(sa.Integer)
    memo = sa.Column(sa.String)


class EnumTypeCode(BaseORM):
    __tablename__ = 'enum_type_code'

    type_code = sa.Column(sa.String(40), primary_key=True)
    type_name = sa.Column(sa.String(300))
    present_column = sa.Column(sa.String(100), index=True)

