# -*- coding: utf-8 -*-
"""
@Time: 2020/6/8 10:18
@Author: Sue Zhu
"""
import importlib

import numpy as np
import pandas as pd
import sqlalchemy as sa

from ._postgres import *
from .comment import get_dates, get_last_td
from .pg_models import monitors
from ..exc import DataExistError
from ..interface import AbstractFactorIO, AbstractFactor


def _sql_name(factor):
    return f"{factor.asset_type.value}_factor_{factor.name}"


def add_factor_to_monitor(module_path, params=None, freq='M', active=1):
    if params is None:
        params = dict()
    module_path, cls = module_path.rsplit('.', maxsplit=1)
    factor = getattr(importlib.import_module(module_path), cls)(**params)
    upsert_data(
        (dict(
            target_table=_sql_name(factor),
            module_path='.'.join((module_path, cls)),
            params=params, status=active,
            calc_freq=freq
        ),),
        monitors.FundFactorList,
        ukeys=monitors.FundFactorList.get_primary_key()
    )


class FactorDBTool(AbstractFactorIO):
    __sql_mapping = {float: sa.Float, str: sa.String(100), int: sa.Integer, np.datetime64: sa.Date}

    def __init__(self, factor: 'AbstractFactor'):
        super().__init__(factor)
        self.__table = None

    @property
    def table(self):
        if self.__table is None:
            self.__table = get_or_create_table(
                _sql_name(self._factor),
                sa.Column('wind_code', sa.String(40), index=True),
                sa.Column('trade_dt', sa.Date, index=True),
                *(sa.Column(name, self.__sql_mapping[tp_]) for name, tp_ in self._factor.field_types.items())
            )
        return self.__table

    def localized_snapshot(self, dt, if_exist=1):
        """
        计算并保存单日因子
        """
        # check if data exist
        with get_session() as session:
            filters = [self.table.c.trade_dt == dt]
            if if_exist == 1:
                res = session.execute(sa.delete(self.table).where(*filters))
                try_commit(
                    session, 'delete exist factor values',
                    f'delete {res.rowcount} from {self.table.key}' if res.rowcount else ''
                )
            elif session.query(self.table).filter(*filters).first():
                msg = f"factor data at {dt:%Y-%m-%d} is exist, please turn into replace mode or check your code!"
                raise DataExistError(msg)

        self.logger.info(f'compute factor at {dt:%Y-%m-%d}')
        snapshot = self._factor.compute(dt)
        if snapshot.empty:
            self.logger.warning(f'empty data at {dt:%Y-%m-%d} need to be check!')
        else:
            snapshot.index.name = 'wind_code'
            bulk_insert(snapshot.reset_index().assign(trade_dt=dt), self.table)

    def fetch_snapshot(self, dt):
        with get_session() as session:
            snapshot = pd.DataFrame(
                session.query(
                    *(self.table.c[col] for col in (*self._factor.field_types.keys(), 'wind_code'))
                ).filter(self.table.c.trade_dt == dt).all()
            ).fillna(np.nan)
        return snapshot.set_index('wind_code').astype(self._factor.field_types, errors='ignore')

    def get_calc_dates(self, start, end, freq):
        # real start date
        if start is None:
            real_start = self._factor.start_date
        else:
            real_start = max((t for t in get_dates(freq) if self._factor.start_date <= t <= pd.Timestamp(start)))
        # real end date
        real_end = min((get_last_td(), pd.Timestamp(end) if end is not None else pd.Timestamp.max))
        # final
        return (t for t in get_dates(freq) if real_start <= t <= real_end)

    def get_max_date(self):
        with get_session() as ss:
            max_dt = pd.to_datetime(pd.DataFrame(
                ss.query(sa.func.max(self.table.c.trade_dt).label('max_dt'))
            ).squeeze())
        return max_dt
