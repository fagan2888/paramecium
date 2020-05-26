# -*- coding: utf-8 -*-
"""
@Time: 2020/5/8 22:09
@Author: Sue Zhu

Code to get python code by crawler
import pandas as pd
tb = pd.read_html('http://tushare.xcsc.com:7173/document/2?doc_id=31')[1]
codes = tb.apply(lambda ser: f"{ser['名称']} = sa.Column(sa.String())  # {ser['描述']} {ser['名称']}", axis=1)
codes.to_clipboard(index=False)

"""
from ._base import *
import sqlalchemy.dialects.postgresql as pg


class AShareDescription(BaseORM):
    """
    A股基本资料
    http://tushare.xcsc.com:7173/document/2?doc_id=25

    - list_board
        434001000:创业板
        434003000:中小企业板
        434004000:主板
        434005000:退市整理股票
        434006000:风险警示股票
        434009000:科创板
    """
    __tablename__ = 'stock_org_description'

    wind_code = sa.Column(sa.String(10), primary_key=True)  # TS代码
    short_name = sa.Column(sa.String(100))  # 证券简称
    pinyin = sa.Column(sa.String())  # 简称拼音
    isin_code = sa.Column(sa.String(40))  # ISIN代码
    exchange = sa.Column(sa.String(4))  # 交易所,SSE:上交所,SZSE:深交所
    list_board = sa.Column(sa.String(10))  # 上市板类型
    list_dt = sa.Column(sa.Date, index=True)  # 上市日期
    delist_dt = sa.Column(sa.Date, index=True)  # 退市日期
    currency = sa.Column(sa.String())  # 货币代码
    is_shsc = sa.Column(sa.Integer)  # 是否在沪股通或深港通范围内,0:否;1:沪股通;2:深股通
    comp_code = sa.Column(sa.String())  # 公司代码
    comp_name = sa.Column(sa.String(100))  # 公司中文名称
    comp_name_en = sa.Column(sa.String(100))  # 公司英文名称


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


class AShareSuspend(BaseORM):
    """
    A股停复牌信息
    http://tushare.xcsc.com:7173/document/2?doc_id=32

    - suspend_type 停牌类型
        444001000:上午停牌
        444002000:下午停牌
        444003000:今起停牌
        444004000:盘中停牌
        444007000:停牌1小时
        444016000:停牌一天

    - 停牌原因代码 reason_type
        204023001: 重大资产重组停牌
        204023002: 披露重大信息停牌
        204023003: 召开股东大会停牌
        204023004: 公共媒体报道停牌
        204023005: 股票价格异常波动停牌
        204023007: 定期报告延期披露停牌
        204023008: 重大差错拒不改正停牌
        204023009: 违法违规被查停牌
        204023010: 违反交易所规则停牌
        204023012: 要约收购停牌
        204023013: 风险警示停牌
    """
    __tablename__ = 'stock_org_suspend'

    oid = sa.Column(pg.UUID, server_default=sa.text('uuid_generate_v4()'), primary_key=True)
    wind_code = sa.Column(sa.String(40))  # ts代码 ts_code
    suspend_date = sa.Column(sa.Date)  # 停牌日期
    suspend_type = sa.Column(sa.Integer)  # 停牌类型代码
    resume_date = sa.Column(sa.Date)  # 复牌日期 resump_date
    change_reason = sa.Column(sa.String(100))  # 停牌原因
    suspend_time = sa.Column(sa.String(100))  # 停复牌时间
    reason_type = sa.Column(sa.String())  # 停牌原因代码 change_reason_type


