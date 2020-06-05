# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 21:00
@Author: Sue Zhu
"""
from .utils import *


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
    full_name = sa.Column(sa.String(100))
    currency = sa.Column(sa.String(10))
    management = sa.Column(sa.String(100))  # 管理人
    custodian = sa.Column(sa.String(100))  # 托管人
    exchange = sa.Column(sa.String(10))

    is_initial = sa.Column(sa.Integer, index=True)
    invest_type = sa.Column(sa.String(40))  # 投资类型fund_type
    invest_style = sa.Column(sa.String(40))  # 投资风格invest_type
    fund_type = sa.Column(sa.String(40))  # 基金类型type

    setup_date = sa.Column(sa.Date, index=True)  # 成立日期found_date
    maturity_date = sa.Column(sa.Date, index=True)  # 到期日期due_date
    status = sa.Column(sa.String(1))  # 存续状态D摘牌 I发行 L已上市
    issue_date = sa.Column(sa.Date)  # 发行日期
    list_date = sa.Column(sa.Date)  # 上市时间
    delist_date = sa.Column(sa.Date)  # 退市日期

    benchmark = sa.Column(sa.String)  # 业绩比较基准
    invest_scope = sa.Column(sa.Text)
    invest_object = sa.Column(sa.Text)

    fee_ratio_manage = sa.Column(sa.Float)  # m_fee  # 管理费
    fee_ratio_custodian = sa.Column(sa.Float)  # c_fee  # 托管费
    fee_ratio_sale = sa.Column(sa.Float)
    purchase_start_dt = sa.Column(sa.Date)  # 日常申购起始日
    redemption_start_dt = sa.Column(sa.Date)  # 日常赎回起始日
    min_purchase_amt = sa.Column(sa.Float)


class MutualFundSale(BaseORM):
    __tablename__ = 'mf_web_sale'

    product_code = sa.Column(sa.String(10), primary_key=True)
    product_id = sa.Column(sa.Integer, index=True)
    product_abbr = sa.Column(sa.String(100))
    risk_level = sa.Column(sa.Integer)
    per_buy_limit = sa.Column(sa.Float)
    product_status = sa.Column(sa.Integer)
    subscribe_start_time = sa.Column(sa.Date)
    subscribe_end_time = sa.Column(sa.Date)
    purchase_rates = sa.Column(sa.Float)
    purchase_rates_dis = sa.Column(sa.Float)


class MutualFundNav(BaseORM):
    __tablename__ = 'mf_org_nav'

    oid = gen_oid()
    wind_code = sa.Column(sa.String(40), index=True)
    ann_date = sa.Column(sa.Date)
    trade_dt = sa.Column(sa.Date, index=True)
    unit_nav = sa.Column(sa.Float)  #
    acc_nav = sa.Column(sa.Float)
    adj_factor = sa.Column(sa.Float)

    net_asset = sa.Column(sa.Float)  # float Y 资产净值
    total_netasset = sa.Column(sa.Float)  # float Y 合计资产净值


class MutualFundManager(BaseORM):
    __tablename__ = 'mf_org_manager'

    oid = gen_oid()
    wind_code = sa.Column(sa.String(40), index=True)  # ts_code str Y 基金代码
    ann_date = sa.Column(sa.Date)  # ann_date str Y 公告日期
    manager_name = sa.Column(sa.String(40))  # name str Y 基金经理姓名
    start_dt = sa.Column(sa.Date, index=True)  # str Y 任职日期begin_date
    end_dt = sa.Column(sa.Date)  # str Y 离任日期end_date

    uk = sa.UniqueConstraint(wind_code, manager_name, start_dt)


class MutualFundSector(BaseORM):
    """
    A股板块信息
    - 中证行业成分(2016): http://tushare.xcsc.com:7173/document/2?doc_id=10212
    - 申万: http://124.232.155.79:7173/document/2?doc_id=10181
    """
    __tablename__ = 'mf_org_sector'

    oid = gen_oid()

    wind_code = sa.Column(sa.String(10), index=True)  # ts代码 ts_code
    sector_code = sa.Column(sa.String(40), index=True)  # 中证行业代码 index_code
    entry_dt = sa.Column(sa.Date, index=True)  # 纳入日期 entry_dt
    remove_dt = sa.Column(sa.Date)  # 剔除日期 remove_dt
