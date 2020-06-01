# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 12:51
@Author: Sue Zhu
"""
from .utils import *


class IndexDescription(BaseORM):
    __tablename__ = 'index_org_description'

    wind_code = sa.Column(sa.String(40), primary_key=True)  # 代码ts_code
    short_name = sa.Column(sa.String(50))  # 证券简称 name
    full_name = sa.Column(sa.String(100))  # 指数名称 comp_name
    market = sa.Column(sa.String(40))  # 交易所或服务商
    base_date = sa.Column(sa.Date)  # 基期 index_base_per
    base_point = sa.Column(sa.Float)  # 基点 index_base_pt
    list_date = sa.Column(sa.Date)  # 发布日期 list_date
    weights_rule = sa.Column(sa.String(10))  # 加权方式 index_weights_rule
    publisher = sa.Column(sa.String(100))  # 发布方 publisher
    index_code = sa.Column(sa.String(10))  # 指数类别代码 index_code
    index_style = sa.Column(sa.String(40))  # 指数风格 index_style
    index_intro = sa.Column(sa.Text)  # 指数简介 index_intro
    weight_type = sa.Column(sa.String(100))  # 权重类型 weight_type
    expire_date = sa.Column(sa.Date)  # 终止发布日期 expire_date
    income_processing_method = sa.Column(sa.Text)  # 收益处理方式 income_processing_method
    change_history = sa.Column(sa.Text)  # 变更历史 change_history


class IndexEODPrice(BaseORM):
    """ 中国A股日行情 """
    __tablename__ = 'index_org_price'

    oid = gen_oid()
    wind_code = sa.Column(sa.String(10), index=True)  # ts代码 ts_code
    trade_dt = sa.Column(sa.Date, index=True)  # 交易日期 trade_date
    open_ = sa.Column(pg.REAL)  # 开盘价(元) open
    high_ = sa.Column(pg.REAL)  # 最高价(元) high
    low_ = sa.Column(pg.REAL)  # 最低价(元) low
    close_ = sa.Column(pg.REAL)  # 收盘价(元) close
    volume_ = sa.Column(sa.Integer)  # 成交量(手) volume
    amount_ = sa.Column(pg.REAL)  # 成交金额(千元) amount
    adj_factor = sa.Column(pg.REAL)  # 复权因子
