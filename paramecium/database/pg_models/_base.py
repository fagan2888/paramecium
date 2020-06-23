# -*- coding: utf-8 -*-
"""
@Time: 2020/6/22 8:45
@Author: Sue Zhu
"""
__all__ = [
    'BaseORM', 'gen_oid', 'gen_update',
    'AbstractDesc', 'AbstractPrice', 'AbstractSector'
]

import sqlalchemy as sa

from .._postgres import BaseORM, gen_oid, gen_update


class AbstractDesc(BaseORM):
    __abstract__ = True

    wind_code = sa.Column(sa.String(40), primary_key=True, comment='证券代码')
    short_name = sa.Column(sa.String(100), comment='证券简称')
    currency = sa.Column(sa.String(40), comment='货币代码')


class AbstractPrice(BaseORM):
    __abstract__ = True

    oid = gen_oid()
    wind_code = sa.Column(sa.String(40), index=True, comment='证券代码')
    trade_dt = sa.Column(sa.Date, index=True, comment='交易日期')
    close_ = sa.Column(sa.Float, comment='收盘价(元)')


class AbstractSector(BaseORM):
    __abstract__ = True

    oid = gen_oid()
    wind_code = sa.Column(type_=sa.String(40), index=True, comment='证券代码')
    sector_code = sa.Column(type_=sa.String(40), index=True)
    entry_dt = sa.Column(type_=sa.Date, comment='开始日期')
    remove_dt = sa.Column(type_=sa.Date, comment='结束日期')
