# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 21:00
@Author: Sue Zhu
"""
from sqlalchemy.dialects.postgresql import UUID

from ._base import *


class MutualFundDescription(BaseORM):
    __tablename__ = 'mf_org_description'

    # wind_code = sa.Column(sa.String(40), primary_key=True)  # 代码
    # short_name = sa.Column(sa.String(100))  # 证券简称
    # full_name = sa.Column(sa.String(100))
    # setup_date = sa.Column(sa.Date)
    # maturity_date = sa.Column(sa.Date)
    # is_index = sa.Column(sa.Integer)
    # fund_type = sa.Column(sa.String(20))
    # invest_type = sa.Column(sa.String(40))
    # grad_type = sa.Column(sa.Integer)
    wind_code = sa.Column(sa.String(40), primary_key=True)  # 代码ts_code
    short_name = sa.Column(sa.String(100))  # 证券简称name
    management = sa.Column(sa.String(100))  # 管理人
    custodian = sa.Column(sa.String(100))  # 托管人
    invest_type = sa.Column(sa.String(40))  # 投资类型fund_type
    setup_date = sa.Column(sa.Date)  # 成立日期found_date
    maturity_date = sa.Column(sa.Date)  # 到期日期due_date
    list_date = sa.Column(sa.Date)  # 上市时间
    issue_date = sa.Column(sa.Date)  # 发行日期
    delist_date = sa.Column(sa.Date)  # 退市日期
    fee_ratio_manage = sa.Column(sa.Float)  # m_fee  # 管理费
    fee_ratio_custodian = sa.Column(sa.Float)  # c_fee  # 托管费
    benchmark = sa.Column(sa.String)  # 业绩比较基准
    status = sa.Column(sa.String(1))  # 存续状态D摘牌 I发行 L已上市
    invest_style = sa.Column(sa.String(40))  # 投资风格invest_type
    fund_type = sa.Column(sa.String(40))  # 基金类型type
    purchase_start_dt = sa.Column(sa.Date)  # 日常申购起始日
    redemption_start_dt = sa.Column(sa.Date)  # 日常赎回起始日


class MutualFundNav(BaseORM):
    __tablename__ = 'mf_org_nav'

    oid = sa.Column(UUID, server_default=sa.text('uuid_generate_v4()'), primary_key=True)
    wind_code = sa.Column(sa.String(40))
    ann_date = sa.Column(sa.Date)
    end_date = sa.Column(sa.Date)
    unit_nav = sa.Column(sa.Float)
    acc_nav = sa.Column(sa.Float)
    adj_nav = sa.Column(sa.Float)
