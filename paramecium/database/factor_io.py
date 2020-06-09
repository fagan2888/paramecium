# -*- coding: utf-8 -*-
"""
@Time: 2020/6/8 10:18
@Author: Sue Zhu
"""

import sqlalchemy.dialects.postgresql as pg

from ._postgres import *
from .comment import get_dates, get_last_td
from ..interface import AbstractFactorIO, AbstractFactor


class FactorDBTool(AbstractFactorIO):
    __sql_mapping = {float: pg.REAL, str: sa.String(100), int: sa.Integer, pd.Timestamp: sa.Date}

    def __init__(self, factor: 'AbstractFactor'):
        super().__init__(factor)
        self.table = get_or_create_table(
            self.table_name,
            sa.Column('wind_code', sa.String(40), index=True),
            sa.Column('trade_dt', sa.Date, index=True),
            *(sa.Column(name, self.__sql_mapping[tp_]) for name, tp_ in self._factor.field_types.items())
        )

    def localized_snapshot(self, dt, if_exist=1):
        """
        计算并保存单日因子
        :param dt: pd.Timestamp
        :param if_exist: {1: replace, 0: error}, 由于因子在截面上有相关性，不考虑update部分的情况
        :return:
        """
        # check if data exist
        with get_session() as session:
            query = session.query(self.table).filter(self.table.c.trade_dt == dt)
            if if_exist == 1:
                query.delete(synchronize_session='fetch')
            else:
                if query.first():
                    msg = f"factor data at {dt:%Y-%m-%d} is exist, please turn into replace mode or check your code!"
                    raise KeyError(msg)

        self.logger.debug(f'compute factor at {dt:%Y-%m-%d}')
        snapshot = self._factor.compute(dt)
        if snapshot.empty:
            self.logger.warning(f'empty data at {dt:%Y-%m-%d} need to be check!')
        else:
            snapshot.index.name = 'wind_code'
            snapshot.assign(trade_dt=dt).to_sql(
                name=self.table_name, con=get_sql_engine('postgres'), method='multi', if_exists='append'
            )

    def fetch_snapshot(self, dt):
        with get_session() as session:
            snapshot = pd.DataFrame(
                session.query(
                    *(self.table.c[col] for col in (*self._factor.field_types.keys(), 'wind_code'))
                ).filter(self.table.c.trade_dt == dt).all()
            ).set_index('wind_code')
        if snapshot.empty:
            snapshot = self.localized_snapshot(dt, if_exist=1)
        return snapshot

    def _get_dates(self, start, end, freq):
        if start is None:
            real_start = self._factor.start_date
        else:
            real_start = max((t for t in get_dates(freq) if self._factor.start_date <= t <= pd.Timestamp(start)))
        real_end = min((get_last_td(), pd.Timestamp(end) if end is not None else pd.Timestamp.max))
        return (t for t in get_dates(freq) if real_start <= t <= real_end)
