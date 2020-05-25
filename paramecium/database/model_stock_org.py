# -*- coding: utf-8 -*-
"""
@Time: 2020/5/8 22:09
@Author: Sue Zhu
"""
from ._base import *
import sqlalchemy.dialects.postgresql as pg


class AShareDescription(BaseORM):
    """
    A股基本资料
    http://tushare.xcsc.com:7173/document/2?doc_id=25
    """
    __tablename__ = 'stock_org_description'

    wind_code = sa.Column(sa.String(10), primary_key=True)  # TS代码
    short_name = sa.Column(sa.String(100))  # 证券简称
    pinyin = sa.Column(sa.String())  # 简称拼音
    isin_code = sa.Column(sa.String(40))  # ISIN代码
    exchange = sa.Column(sa.String(4))  # 交易所,SSE:上交所,SZSE:深交所
    list_board = sa.Column(sa.String(10))  # 上市板类型,见下
    list_dt = sa.Column(sa.Date, index=True)  # 上市日期
    delist_dt = sa.Column(sa.Date, index=True)  # 退市日期
    currency = sa.Column(sa.String())  # 货币代码
    is_shsc = sa.Column(sa.Integer)  # 是否在沪股通或深港通范围内,0:否;1:沪股通;2:深股通
    comp_code = sa.Column(sa.String())  # 公司代码
    comp_name = sa.Column(sa.String(100))  # 公司中文名称
    comp_name_en = sa.Column(sa.String(100))  # 公司英文名称

    # 434001000:创业板
    # 434003000:中小企业板
    # 434004000:主板
    # 434005000:退市整理股票
    # 434006000:风险警示股票
    # 434009000:科创板


class AShareEODPrice(BaseORM):
    """
    中国A股日行情
    http://tushare.xcsc.com:7173/document/2?doc_id=27
    """
    __tablename__ = 'stock_org_price'

    oid = sa.Column(pg.UUID, server_default=sa.text('uuid_generate_v4()'), primary_key=True)
    wind_code = sa.Column(sa.String(10), index=True)  # ts代码 ts_code
    trade_dt = sa.Column(sa.Date)  # 交易日期 trade_date
    open_ = sa.Column(pg.REAL)  # 开盘价(元) open
    high_ = sa.Column(pg.REAL)  # 最高价(元) high
    low_ = sa.Column(pg.REAL)  # 最低价(元) low
    close_ = sa.Column(pg.REAL)  # 收盘价(元) close
    volume_ = sa.Column(sa.Integer)  # 成交量(手) volume
    amount_ = sa.Column(pg.REAL)  # 成交金额(千元) amount
    adj_factor = sa.Column(pg.REAL)  # 复权因子
    avg_price = sa.Column(pg.REAL)  # 均价(VWAP)
    trade_status = sa.Column(sa.String(10))  # 交易状态

    sa.UniqueConstraint(trade_dt, wind_code, name=f'ix_{__tablename__}_dt_code')
