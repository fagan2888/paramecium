# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 10:50
@Author: Sue Zhu
"""
from functools import lru_cache
from itertools import product

import pandas as pd
import sqlalchemy as sa

from paramecium.const import FreqEnum, AssetEnum
from paramecium.database import get_dates, get_price
from paramecium.database.factor_io import FactorDBTool
from paramecium.database.index_derivative import IndexDerivativeDesc, IndexDerivativePrice
from paramecium.factor_pool.stock_classic import FamaFrench
from paramecium.scheduler_.jobs._localizer import BaseLocalizerJob


class StockFF3Factor(BaseLocalizerJob):
    """
    Localized Stock Fama-French Data
    """
    meta_args = (
        {'type': 'string', 'description': 'date string that can be translate by pd.to_datetime'}
    )

    def __init__(self, job_id=None, execution_id=None):
        super().__init__(job_id, execution_id)
        self.ff3 = FamaFrench()
        self.io = FactorDBTool(self.ff3)

    def run(self, start=None, *args, **kwargs):
        self.upsert_data(
            (dict(
                benchmark_code=f'Stock_FF3_{s}{g}',
                benchmark_name=f'股票{sn}盘{gn}指数',
                base_date=self.ff3.start_date,
                base_point=1e3,
                updated_at=sa.func.current_timestamp(),
            ) for (s, g), (sn, gn) in zip(product('SMB', 'GNV'), product('小中大', ('成长', '平衡', '价值')))),
            IndexDerivativeDesc,
            ukeys=[IndexDerivativeDesc.benchmark_code],
            msg='benchmark code into description table'
        )
        if start is None:
            start = self.ff3.start_date

        with self.get_session() as session:
            self.get_logger().debug('query max date')
            max_dt, = session.query(
                sa.func.max(IndexDerivativePrice.trade_dt),
            ).filter(
                IndexDerivativePrice.benchmark_code == 'Stock_FF3_SG'
            ).one()

            if max_dt is None:
                self.get_logger().debug('`max_dt` is None, run from start.')
                max_dt = self.ff3.start_date
                self.bulk_insert(
                    (dict(
                        benchmark_code=f'Stock_FF3_{s}{g}',
                        trade_dt=self.ff3.start_date,
                        close_=1e3
                    ) for s, g in product('SMB', 'GNV')),
                    IndexDerivativePrice,
                )
                self.io.localized_snapshot(max_dt, if_exist=1)
            else:
                max_dt = max((self.ff3.start_date, min(start, pd.to_datetime(max_dt) - pd.Timedelta(days=7))))
                self.get_logger().debug(f'`max_dt` is not None, run from {max_dt:%Y-%m-%d}.')
                session.query(IndexDerivativePrice).filter(
                    IndexDerivativePrice.benchmark_code.in_([f'Stock_FF3_{s}{g}' for s, g in product('SMB', 'GNV')]),
                    IndexDerivativePrice.trade_dt > max_dt
                ).delete(synchronize_session='fetch')

            month_end = max((t for t in get_dates(FreqEnum.M) if t <= max_dt))
            close_cache = {month_end: pd.DataFrame(
                session.query(
                    IndexDerivativePrice.benchmark_code,
                    IndexDerivativePrice.close_,
                ).filter(
                    IndexDerivativePrice.trade_dt == month_end
                ).all(),
                columns=['benchmark_code', 'close_']
            ).set_index('benchmark_code').squeeze()}
            trade_dates = (t for t in get_dates(FreqEnum.D) if max_dt < t <= self.last_td_date)

        for dt in trade_dates:
            self.get_logger().debug(f'run at {max_dt:%Y-%m-%d}.')
            month_end = max((t for t in get_dates(FreqEnum.M) if t < dt))
            factor = self._get_snap(dt).assign(
                ret=self.get_adj_close(dt).div(self.get_adj_close(month_end)) - 1
            )
            cum_ret = factor.groupby('label').apply(
                lambda df: df['ret'].mul(df['capt'].div(df['capt'].sum())).sum()
            )
            nav = close_cache[month_end].mul(cum_ret.add(1))
            self.bulk_insert(
                records=[dict(
                    benchmark_code=f'Stock_FF3_{k}',
                    trade_dt=dt,
                    close_=v,
                ) for k, v in nav.items()],
                model=IndexDerivativePrice,
                msg=f'{dt:%Y-%m-%d}'
            )

            if dt == month_end:
                self.get_logger().debug(f'localized factor and close at {max_dt:%Y-%m-%d}.')
                self.io.localized_snapshot(max_dt, if_exist=1)
                close_cache[month_end] = nav

    @staticmethod
    def get_bm_close(dt, session):
        close_ = pd.DataFrame(
            session.query(
                IndexDerivativePrice.benchmark_code,
                IndexDerivativePrice.close_,
            ).filter(IndexDerivativePrice.trade_dt == dt).all()
        ).set_index('benchmark_code').squeeze()
        return close_

    @lru_cache(maxsize=1)
    def _get_snap(self, dt):
        factor = self.io.fetch_snapshot(dt)
        return factor

    @staticmethod
    @lru_cache(maxsize=32)
    def get_adj_close(dt):
        price = get_price(AssetEnum.STOCK, start=dt, end=dt).set_index('wind_code')
        return price['close_'].mul(price['adj_factor'])


if __name__ == '__main__':
    StockFF3Factor().run()
