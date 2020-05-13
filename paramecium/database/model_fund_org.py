# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 21:00
@Author: Sue Zhu
"""
from sqlalchemy.dialects.postgresql import UUID

from ._base import *


class MutualFundDescription(BaseORM):
    __tablename__ = 'mf_org_description'

    wind_code = sa.Column(sa.String(40), primary_key=True)  # 代码
    short_name = sa.Column(sa.String(100))  # 证券简称
    full_name = sa.Column(sa.String(100))
    setup_date = sa.Column(sa.Date)
    maturity_date = sa.Column(sa.Date)
    is_index = sa.Column(sa.Integer)
    fund_type = sa.Column(sa.String(20))
    invest_type = sa.Column(sa.String(40))
    grad_type = sa.Column(sa.Integer)


class MutualFundNav(BaseORM):
    __tablename__ = 'mf_org_nav'

    oid = sa.Column(UUID, server_default=sa.text('uuid_generate_v4()'), primary_key=True)
    wind_code = sa.Column(sa.String(40))
    ann_date = sa.Column(sa.Date)
    end_date = sa.Column(sa.Date)
    unit_nav = sa.Column(sa.Float)
    acc_nav = sa.Column(sa.Float)
    adj_nav = sa.Column(sa.Float)
