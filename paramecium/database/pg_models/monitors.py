# -*- coding: utf-8 -*-
"""
@Time: 2020/6/11 14:43
@Author: Sue Zhu
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

from .._postgres import *


class FundFactorList(BaseORM):
    __tablename__ = 'monitor_factor_list'

    oid = gen_oid()
    module_path = sa.Column(sa.String(400), index=True)
    params = sa.Column(pg.JSONB)
    calc_freq = sa.Column(sa.String(1), index=True)
    status = sa.Column(sa.Integer, server_default=sa.text('1'), index=True)
