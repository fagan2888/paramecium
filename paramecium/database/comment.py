# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:54
@Author: Sue Zhu
"""
__all__ = [
    'get_dates', 'get_last_td_date',
    'get_risk_free_rates',
    'get_price', 'get_sector'
]

from functools import lru_cache

import numpy as np
import pandas as pd
import sqlalchemy as sa

from ._postgres import get_session, macro, get_table_by_name, get_dates, get_last_td_date
from ..const import FreqEnum, AssetEnum


@lru_cache()
def get_basic_rates(type_='save'):
    with get_session() as ss:
        query_df = ss.query(
            macro.InterestRate.change_dt.label('trade_dt'),
            getattr(macro.InterestRate, type_)
        ).all()

    return {k: v / 100 for k, v in query_df.items()}


def get_risk_free_rates(type_='save', freq=FreqEnum.D):
    basic_rates = pd.Series(get_basic_rates(type_)).rename(index=pd.to_datetime)
    rf = basic_rates.resample('D').ffill().add(1).pow(1 / freq.value).sub(1)
    return rf.filter(items=get_dates(freq))


def get_price(asset: AssetEnum, start=None, end=None, code=None, fields=None):
    tb_dict = {
        AssetEnum.STOCK: 'stock_org_price',
        AssetEnum.CMF: 'mf_org_nav',
        AssetEnum.INDEX: 'index_price',
    }
    model = get_table_by_name(tb_dict[asset])

    filters = []
    if start and end and start == end:
        filters.append(model.trade_dt == start)
    else:
        if start:
            filters.append(model.trade_dt >= start)
        if end:
            filters.append(model.trade_dt <= end)
    if code:
        filters.append(model.wind_code == code)

    if fields:
        sa_fields = (getattr(model, c) for c in {*fields, 'wind_code', 'trade_dt'})
    else:
        sa_fields = (c for c in model.__table__.c if c.key not in ('oid', 'updated_at'))

    with get_session() as session:
        data = pd.DataFrame(session.query(*sa_fields).filter(*filters).all()).fillna(np.nan)

    data.loc[:, 'trade_dt'] = pd.to_datetime(data['trade_dt'])

    return data


def get_sector(asset: AssetEnum, valid_dt=None, sector_type=None):
    table = get_table_by_name(f'{asset.value}_org_sector')

    filters = []
    if valid_dt:
        filters.extend((valid_dt >= table.c.entry_dt, valid_dt <= table.c.remove_dt))
    if sector_type:
        start_code = sector_type.value
        filters.append(
            sa.func.substr(table.c.sector_code, 1, len(start_code)) == start_code
        )
    with get_session() as session:
        data = pd.DataFrame(session.query(table).filter(*filters).all()).fillna(np.nan)
        for t_col in ('entry_dt', 'remove_dt'):
            data.loc[:, t_col] = pd.to_datetime(data[t_col])
        data.drop(['oid', 'updated_at'], axis=1, errors='ignore')

    return data
