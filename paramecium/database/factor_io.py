# -*- coding: utf-8 -*-
"""
@Time: 2020/6/8 10:18
@Author: Sue Zhu
"""
import pandas as pd
import sqlalchemy as sa

from .comment import get_dates, get_last_td
from ._postgres import get_session, get_sql_engine
from ..interface import AbstractFactorIO, AbstractFactor


class FactorDBTool(AbstractFactorIO):

    def __init__(self, factor: 'AbstractFactor'):
        super().__init__(factor)
        with get_session() as session:
            if not session.bind.has_table(self.table_name):
                self.logger.info('Factor Table do not exist, create it first.')
                self._factor.get_empty_table().to_sql(**self.saving_param, index=False)
                session.execute(
                    f"""
                    alter table {self.table_name} add oid uuid default uuid_generate_v4();
                    alter table {self.table_name} add updated_at timestamp default current_timestamp;
                    alter table {self.table_name} add constraint pk_{self.table_name} primary key(oid);
                    
                    create index ix_{self.table_name}_code on {self.table_name} (wind_code);
                    create index ix_{self.table_name}_dt on {self.table_name} (trade_dt);
                    """
                )

    @property
    def saving_param(self):
        return dict(
            name=self.table_name, con=get_sql_engine('_models'),
            dtype={'wind_code': sa.String(40), 'trade_dt': sa.Date, **self._factor.field_types}
        )

    def localized_snapshot(self, dt, if_exist=1):
        """
        计算并保存单日因子
        :param dt: pd.Timestamp
        :param if_exist: {1: replace, 0: error}, 由于因子在截面上有相关性，不考虑update部分的情况
        :return:
        """
        # check if data exist
        if if_exist == 1:
            with get_session() as session:
                session.execute(f"delete from {self.table_name} where trade_dt='{dt:%Y-%m-%d}'")
        else:
            exist_rows = pd.read_sql(
                f"select count(*) from {self.table_name} where trade_dt='{dt:%Y-%m-%d}'",
                get_sql_engine('_models'),
            ).squeeze()
            if exist_rows:
                msg = f"factor data at {dt:%Y-%m-%d} is exist, please turn into replace mode or check your code!"
                self.logger.error(msg)
                raise KeyError(msg)

        self.logger.debug(f'compute factor at {dt:%Y-%m-%d}')
        snapshot = self._factor.compute(dt)
        if snapshot.empty:
            self.logger.warning(f'empty data at {dt:%Y-%m-%d} need to be check!')
        else:
            snapshot.index.name = 'wind_code'
            snapshot.assign(trade_dt=dt).to_sql(**self.saving_param, method='multi', if_exists='append')

    def fetch_snapshot(self, dt):
        snapshot = pd.read_sql(
            f"select * from {self.table_name} where trade_dt='{dt:%Y-%m-%d}'",
            con=get_sql_engine('_models'), index_col=['wind_code'],
        ).drop(['updated_at', 'oid', 'trade_dt'], errors='ignore', axis=1)
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