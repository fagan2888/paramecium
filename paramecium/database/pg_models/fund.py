# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 21:00
@Author: Sue Zhu
"""
import sqlalchemy as sa

from .._postgres import *


class Description(BaseORM):
    __tablename__ = 'mf_org_description'

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


class WebSaleList(BaseORM):
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


class Nav(BaseORM):
    __tablename__ = 'mf_org_nav'

    oid = gen_oid_col()
    wind_code = sa.Column(sa.String(40), index=True)
    ann_date = sa.Column(sa.Date)
    trade_dt = sa.Column(sa.Date, index=True)
    unit_nav = sa.Column(sa.Float)  #
    acc_nav = sa.Column(sa.Float)
    adj_factor = sa.Column(sa.Float)

    net_asset = sa.Column(sa.Float)  # float Y 资产净值
    total_netasset = sa.Column(sa.Float)  # float Y 合计资产净值


class ManagerHistory(BaseORM):
    __tablename__ = 'mf_org_manager'

    oid = gen_oid_col()
    wind_code = sa.Column(sa.String(40), index=True)  # ts_code str Y 基金代码
    ann_date = sa.Column(sa.Date)  # ann_date str Y 公告日期
    manager_name = sa.Column(sa.String(40))  # name str Y 基金经理姓名
    start_dt = sa.Column(sa.Date, index=True)  # str Y 任职日期begin_date
    end_dt = sa.Column(sa.Date)  # str Y 离任日期end_date

    uk = sa.UniqueConstraint(wind_code, manager_name, start_dt)


class Sector(BaseORM):
    """
    基金板块信息
    """
    __tablename__ = 'mf_org_sector'

    oid = gen_oid_col()

    wind_code = sa.Column(sa.String(10), index=True)  # ts代码 ts_code
    sector_code = sa.Column(sa.String(40), index=True)  # 中证行业代码 index_code
    entry_dt = sa.Column(sa.Date, index=True)  # 纳入日期 entry_dt
    remove_dt = sa.Column(sa.Date)  # 剔除日期 remove_dt


class SectorSnapshot(BaseORM):
    """
    基金板块信息（月度截面）
    """
    __tablename__ = 'mf_org_sector_m'

    oid = gen_oid_col()
    wind_code = sa.Column(sa.String(40), index=True)
    sector_code = sa.Column(sa.String(40), index=True)
    trade_dt = sa.Column(sa.Date, index=True)
    type_ = sa.Column(sa.String(4), index=True)


class Connections(BaseORM):
    """
    关联基金列表
    目前主要包含联接基金和分级基金
    """
    __tablename__ = 'mf_org_connections'

    oid = gen_oid_col()
    connect_type = sa.Column(sa.String(1), index=True)  # 关系代码 {'F':联接基金, 'A':分级A, 'B':分级B}
    parent_code = sa.Column(sa.String(40), index=True)
    child_code = sa.Column(sa.String(40))

    uk = sa.UniqueConstraint(connect_type, parent_code, name=f'uk_mf_connection_codes')


class Converted(BaseORM):
    """
    转型基金
    基金公告手工筛选 "转型为"
    """
    __tablename__ = 'mf_org_convert'

    wind_code = sa.Column(sa.String(40), primary_key=True)
    ann_date = sa.Column(sa.Date)
    chg_date = sa.Column(sa.Date, index=True)
    memo = sa.Column(sa.Text)


class PortfolioAsset(BaseORM):
    """
    基金资产配置
    """
    __tablename__ = 'mf_org_portfolio'

    oid = gen_oid_col()
    wind_code = sa.Column(sa.String(40), index=True)  # S_INFO_WINDCODE Wind代码
    end_date = sa.Column(sa.Date, index=True)  # F_PRT_ENDDATE 截止日期
    total_asset = sa.Column(sa.Float)  # F_PRT_TOTALASSET 资产总值(元)
    net_asset = sa.Column(sa.Float)  # F_PRT_NETASSET 资产净值(元)

    stock_value = sa.Column(sa.Float)  # F_PRT_STOCKVALUE 持有股票市值(元)
    stock_passive = sa.Column(sa.Float)  # F_PRT_PASVSTKVALUE 指数投资持有股票市值(元)
    stock_positive = sa.Column(sa.Float)  # F_PRT_POSVSTKVALUE 积极投资持有股票市值(元)
    stock_hk = sa.Column(sa.Float)  # F_PRT_HKSTOCKVALUE 港股通投资港股市值

    bond_value = sa.Column(sa.Float)  # F_PRT_BONDVALUE 持有债券市值总计(元)
    fund_value = sa.Column(sa.Float)  # F_PRT_FUNDVALUE 持有基金市值(元)
    warrant_value = sa.Column(sa.Float)  # F_PRT_WARRANTVALUE 持有权证市值(元)
    mm_value = sa.Column(sa.Float)  # F_PRT_MMVALUE 持有货币市场工具市值(元)
    cash = sa.Column(sa.Float)  # F_PRT_CASH 持有现金(元)
    other = sa.Column(sa.Float)  # F_PRT_OTHER 持有其他资产(元)

    mmf_avgptm = sa.Column(sa.Float)  # F_MMF_AVGPTM 投资组合平均剩余期限(天)
    mmf_reverserepo = sa.Column(sa.Float)  # F_MMF_REVERSEREPO 买入返售证券(元)
    mmf_repo = sa.Column(sa.Float)  # F_MMF_REPO 卖出回购证券(元)

    ann_date = sa.Column(sa.Date)  # F_ANN_DATE 公告日期
    dr_cr_rebalance = sa.Column(sa.Float)  # F_PRT_DEBCREBALANCE 借贷方差额(元)
    abs_value = sa.Column(sa.Float)  # F_PRT_ABSVALUE 持有资产支持证券市值(元)


class PortfolioAssetBond(BaseORM):
    """
    基金持有券种分布
    """
    __tablename__ = 'mf_org_bond'

    oid = gen_oid_col()
    wind_code = sa.Column(sa.String(40), index=True)  # S_INFO_WINDCODE Wind代码
    end_date = sa.Column(sa.Date, index=True)  # F_PRT_ENDDATE 截止日期
    government = sa.Column(sa.Float)  # F_PRT_GOVBOND 持有国债市值(元)
    no_gov = sa.Column(sa.Float)  # F_PRT_BDVALUE_NOGOV 持有债券市值(不含国债)(元)
    financial = sa.Column(sa.Float)  # F_PRT_FINANBOND 持有金融债市值(元)
    convert = sa.Column(sa.Float)  # F_PRT_COVERTBOND 持有可转债市值(元)
    corp = sa.Column(sa.Float)  # F_PRT_CORPBOND 持有企债市值(元)
    center_bank = sa.Column(sa.Float)  # F_PRT_CTRBANKBILL 持有央行票据市值(元)
    politic_financial = sa.Column(sa.Float)  # F_PRT_POLIFINANBDVALUE 持有政策性金融债市值(元)
    short_term_cp = sa.Column(sa.Float)  # F_PRT_CPVALUE 持有短期融资券市值(元)
    mid_term_cp = sa.Column(sa.Float)  # F_PRT_MTNVALUE 持有中期票据市值(元)
    cds = sa.Column(sa.Float)  # F_PRT_CDS 持有同业存单市值(元)
