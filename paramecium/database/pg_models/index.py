# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 12:51
@Author: Sue Zhu
"""
import sqlalchemy as sa

from ._base import *


class Description(AbstractDesc):
    """ 中国A股指数基本资料 """
    __tablename__ = 'index_org_description'

    full_name = sa.Column(sa.String(100))  # 指数名称 comp_name
    market = sa.Column(sa.String(40), index=True)  # 交易所或服务商
    base_date = sa.Column(sa.Date)  # 基期 index_base_per
    base_point = sa.Column(sa.Float)  # 基点 index_base_pt
    list_date = sa.Column(sa.Date)  # 发布日期 list_date
    weights_rule = sa.Column(sa.String(10))  # 加权方式 index_weights_rule
    publisher = sa.Column(sa.String(100))  # 发布方 publisher
    index_code = sa.Column(sa.String(10), index=True, server_default='647000000')  # 指数类别代码 index_code
    index_style = sa.Column(sa.String(40))  # 指数风格 index_style
    index_intro = sa.Column(sa.Text)  # 指数简介 index_intro
    weight_type = sa.Column(sa.String(100))  # 权重类型 weight_type
    expire_date = sa.Column(sa.Date, index=True)  # 终止发布日期 expire_date
    income_processing_method = sa.Column(sa.Text)  # 收益处理方式
    change_history = sa.Column(sa.Text)  # 变更历史 change_history
    localized = sa.Column(sa.Integer, default=0, index=True)


class EODPrice(AbstractPrice):
    """ A股指数日行情 """
    __tablename__ = 'index_org_price'

    open_ = sa.Column(sa.Float)  # 开盘价(元) open
    high_ = sa.Column(sa.Float)  # 最高价(元) high
    low_ = sa.Column(sa.Float)  # 最低价(元) low
    volume_ = sa.Column(sa.REAL)  # 成交量(手) volume
    amount_ = sa.Column(sa.Float)  # 成交金额(千元) amount


class DerivativeDesc(BaseORM):
    __tablename__ = 'index_derivative_description'

    benchmark_code = sa.Column(sa.String(40), primary_key=True)
    benchmark_name = sa.Column(sa.String(40))
    base_date = sa.Column(sa.Date)
    base_point = sa.Column(sa.Float)


class DerivativePrice(AbstractPrice):
    __tablename__ = 'index_derivative_price'
