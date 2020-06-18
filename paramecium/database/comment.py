# -*- coding: utf-8 -*-
"""
@Time: 2020/6/7 10:54
@Author: Sue Zhu
"""
from functools import lru_cache

import numpy as np
import pandas as pd
import sqlalchemy as sa

from ._postgres import get_session, get_or_create_table
from ._tool import flat_1dim
from .pg_models import others
from ..const import FreqEnum, AssetEnum


@lru_cache()
def get_dates(freq=None):
    """
    获取A股交易日

    :param freq: str or FreqEnum
    :return: DatetimeIndex
    """
    with get_session() as session:
        query = session.query(others.TradeCalendar.trade_dt)
        if freq:
            if isinstance(freq, FreqEnum):
                freq = freq.name
            query = query.filter(getattr(others.TradeCalendar, f'is_{freq.lower()}') == 1)
        data = flat_1dim(query.all())
    return pd.to_datetime(sorted(data))


def get_last_td() -> pd.Timestamp:
    """
    获取最新交易日，考虑到数据更新延迟，23点前认为是前一天，之后包含当天
    """
    cur_date = pd.Timestamp.now()
    if cur_date.hour <= 22:
        cur_date -= pd.Timedelta(days=1)
    return max((t for t in get_dates(freq=FreqEnum.D) if t <= cur_date))


@lru_cache()
def _basic_rates(type_='save'):
    with get_session() as ss:
        query_df = ss.query(
            others.InterestRate.change_dt.label('trade_dt'),
            getattr(others.InterestRate, f'{type_}_rate')
        ).all()

    return {pd.Timestamp(t): (rate / 100) for t, rate in query_df}


def get_risk_free_rates(type_='save', freq=FreqEnum.D):
    basic_rates = pd.Series(_basic_rates(type_))
    daily_rates = basic_rates.reindex(index=pd.date_range('1900-01-01', pd.Timestamp.now(), freq='D'))
    rf = daily_rates.ffill().bfill().add(1).pow(1 / freq.value).sub(1)
    return rf.filter(items=get_dates(freq))


def get_instruments_info(asset: AssetEnum):
    tb_dict = {
        AssetEnum.STOCK: 'stock_org_description',
        AssetEnum.CMF: 'mf_org_description',
        AssetEnum.INDEX: 'index_org_description',
    }
    model = get_or_create_table(name=tb_dict[asset])

    with get_session() as session:
        data = session.query(*(c for c in model.c if c.key not in ('oid', 'updated_at'))).all()

    return pd.DataFrame(data).fillna(np.nan).set_index('wind_code')


def get_price(asset: AssetEnum, start=None, end=None, code=None, fields=None):
    tb_dict = {
        AssetEnum.STOCK: 'stock_org_price',
        AssetEnum.CMF: 'mf_org_nav',
        AssetEnum.INDEX: 'index_price',
    }
    model = get_or_create_table(name=tb_dict[asset])

    filters = []
    if start and end and start == end:
        filters.append(model.c.trade_dt == start)
    else:
        if start:
            filters.append(model.c.trade_dt >= start)
        if end:
            filters.append(model.c.trade_dt <= end)
    if code:
        filters.append(model.c.wind_code == code)

    if fields:
        sa_fields = (getattr(model.c, c) for c in {*fields, 'wind_code', 'trade_dt'})
    else:
        sa_fields = (c for c in model.c if c.key not in ('oid', 'updated_at'))

    with get_session() as session:
        data = pd.DataFrame(session.query(*sa_fields).filter(*filters).all()).fillna(np.nan)

    data.loc[:, 'trade_dt'] = pd.to_datetime(data['trade_dt'])

    return data.sort_values('trade_dt')


def get_sector(asset: AssetEnum, valid_dt, sector_prefix=None):
    if asset == AssetEnum.STOCK:
        model = get_or_create_table(name=f'{asset.value}_org_sector')
        filters = [
            valid_dt >= model.c.entry_dt,
            valid_dt <= model.c.remove_dt
        ]
        if sector_prefix:
            filters.append(sa.func.substr(model.sector_code, 1, len(sector_prefix)) == sector_prefix)
    elif asset == AssetEnum.CMF:
        # Temporary solution
        model = get_or_create_table(name='mf_org_sector_m')
        filters = [valid_dt == model.c.trade_dt]
        if sector_prefix:
            filters.append(model.c.type_ == sector_prefix)
    else:
        raise KeyError(f"Unknown asset type {asset}.")

    with get_session() as session:
        sa_fields = (c for c in model.c if c.key not in ('oid', 'updated_at'))
        data = pd.DataFrame(session.query(*sa_fields).filter(*filters).all()).fillna(np.nan)
        for t_col in {'entry_dt', 'remove_dt', 'trade_dt'} & {*data.columns}:
            data.loc[:, t_col] = pd.to_datetime(data[t_col])

    return data
