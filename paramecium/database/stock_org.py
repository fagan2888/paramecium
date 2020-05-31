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
from .utils import *
import sqlalchemy.dialects.postgresql as pg


class AShareDescription(BaseORM):
    """
    A股基本资料
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
    """ 中国A股日行情 """
    __tablename__ = 'stock_org_price'

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
    avg_price = sa.Column(pg.REAL)  # 均价(VWAP)
    trade_status = sa.Column(sa.Integer, index=True)  # 交易状态


class AShareSuspend(BaseORM):
    """ A股停复牌信息 """
    __tablename__ = 'stock_org_suspend'

    oid = gen_oid()
    wind_code = sa.Column(sa.String(10), index=True)  # ts代码 ts_code
    suspend_date = sa.Column(sa.Date, index=True)  # 停牌日期
    suspend_type = sa.Column(sa.Integer, index=True)  # 停牌类型代码
    resume_date = sa.Column(sa.Date, index=True)  # 复牌日期 resump_date
    change_reason = sa.Column(sa.String(40))  # 停牌原因
    suspend_time = sa.Column(sa.String(100))  # 停复牌时间
    reason_type = sa.Column(sa.String(40))  # 停牌原因代码 change_reason_type


class AShareEODDerivativeIndicator(BaseORM):
    """ A股日行情估值指标 """
    __tablename__ = 'stock_org_eod_derivative'

    oid = gen_oid()

    wind_code = sa.Column(sa.String(10), index=True)  # ts代码 ts_code
    trade_dt = sa.Column(sa.Date, index=True)  # 交易日期 trade_date

    pe = sa.Column(pg.REAL)  # 市盈率(PE)(总市值/净利润(若净利润<=0,则返回空))
    pb_new = sa.Column(pg.REAL)  # 市净率(PB)(总市值/净资产(LF))
    pe_ttm = sa.Column(pg.REAL)  # 市盈率(PE,TTM)(总市值/净利润(TTM))
    pcf_ocf = sa.Column(pg.REAL)  # 市现率(PCF,经营现金流)
    pcf_ocf_ttm = sa.Column(pg.REAL)  # 市现率(PCF,经营现金流TTM)
    pcf_ncf = sa.Column(pg.REAL)  # 市现率(PCF,现金净流量)
    pcf_ncf_ttm = sa.Column(pg.REAL)  # 市现率(PCF,现金净流量TTM)
    ps = sa.Column(pg.REAL)  # 市销率(PS)
    ps_ttm = sa.Column(pg.REAL)  # 市销率(PS,TTM)

    share_tot = sa.Column(pg.REAL)  # 当日总股本(万股) tot_shr
    share_float = sa.Column(pg.REAL)  # 当日流通股本(万股) float_a_shr
    share_free = sa.Column(pg.REAL)  # 当日自由流通股本(万股) free_shares

    turnover = sa.Column(pg.REAL)  # 换手率 turn
    turnover_free = sa.Column(pg.REAL)  # 换手率(基准.自由流通股本) free_turnover

    price_div_dps = sa.Column(pg.REAL)  # 股价/每股派息

    close = sa.Column(pg.REAL)  # 当日收盘价
    suspend_status = sa.Column(sa.Integer, index=True)
    # 涨跌停状态(1表示涨停;0表示非涨停或跌停;-1表示跌停) up_down_limit_status

    price_high_52w = sa.Column(pg.REAL)  # 52周最高价 high_52w
    price_low_52w = sa.Column(pg.REAL)  # 52周最低价 low_52w
    adj_high_52w = sa.Column(pg.REAL)  # 52周最高价(复权) adj_high_52w
    adj_low_52w = sa.Column(pg.REAL)  # 52周最低价(复权) adj_low_52w

    net_assets = sa.Column(pg.REAL)  # 当日净资产
    net_profit_parent_comp_ttm = sa.Column(pg.REAL)  # 归属母公司净利润(TTM)
    net_profit_parent_comp_lyr = sa.Column(pg.REAL)  # 归属母公司净利润(LYR)
    net_cash_flows_oper_act_ttm = sa.Column(pg.REAL)  # 经营活动产生的现金流量净额(TTM)
    net_cash_flows_oper_act_lyr = sa.Column(pg.REAL)  # 经营活动产生的现金流量净额(LYR)
    oper_rev_ttm = sa.Column(pg.REAL)  # 营业收入(TTM)
    oper_rev_lyr = sa.Column(pg.REAL)  # 营业收入(LYR)
    net_increase_cash_equ_ttm = sa.Column(pg.REAL)  # 现金及现金等价物净增加额(TTM) net_incr_cash_cash_equ_ttm
    net_increase_cash_equ_lyr = sa.Column(pg.REAL)  # 现金及现金等价物净增加额(LYR) net_incr_cash_cash_equ_lyr


class AShareSector(BaseORM):
    """
    A股板块信息
    - 中证行业成分(2016): http://tushare.xcsc.com:7173/document/2?doc_id=10212
    - 申万: http://124.232.155.79:7173/document/2?doc_id=10181
    """
    __tablename__ = 'stock_org_sector'

    oid = gen_oid()

    wind_code = sa.Column(sa.String(10), index=True)  # ts代码 ts_code
    sector_code = sa.Column(sa.String(40), index=True)  # 中证行业代码 index_code
    entry_dt = sa.Column(sa.Date)  # 纳入日期 entry_dt
    remove_dt = sa.Column(sa.Date)  # 剔除日期 remove_dt

    u_key = sa.UniqueConstraint(wind_code, sector_code, entry_dt, name=f"uk_{__tablename__}")
