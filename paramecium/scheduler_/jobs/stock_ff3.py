# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 10:50
@Author: Sue Zhu
"""
from itertools import product

import pandas as pd
import sqlalchemy as sa

from paramecium.const import FreqEnum, AssetEnum
from paramecium.database import get_dates, get_price, create_all_table
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
    prefix = 'ff3_'

    def __init__(self, job_id=None, execution_id=None):
        super().__init__(job_id, execution_id)
        self.ff3 = FamaFrench()
        self.io = FactorDBTool(self.ff3)

    def run(self, start=None, *args, **kwargs):
        # insert desc first
        self.upsert_data(
            (dict(
                benchmark_code=f'{self.prefix}{s}{g}',
                benchmark_name=f'股票{sn}盘{gn}指数',
                base_date=self.ff3.start_date,
                base_point=1e3,
                updated_at=sa.func.current_timestamp(),
            ) for (s, g), (sn, gn) in zip(product('SMB', 'GNV'), product('小中大', ('成长', '平衡', '价值')))),
            IndexDerivativeDesc,
            ukeys=[IndexDerivativeDesc.benchmark_code],
            msg='benchmark code into description table'
        )

        # make sure start is not None
        if start is None:
            start = self.ff3.start_date

        with self.get_session() as session:
            # find true start date
            self.get_logger().debug('query max date')
            real_start, = session.query(
                sa.func.max(IndexDerivativePrice.trade_dt),
            ).filter(
                IndexDerivativePrice.benchmark_code == f'{self.prefix}SG'
            ).one()

            if real_start is None:
                # table is empty, so set first nav as base point.
                self.get_logger().debug('`max_dt` is None, run from start.')
                real_start = self.ff3.start_date
                self.bulk_insert(
                    (dict(
                        benchmark_code=f'{self.prefix}{s}{g}',
                        trade_dt=self.ff3.start_date,
                        close_=1e3
                    ) for s, g in product('SMB', 'GNV')),
                    IndexDerivativePrice,
                )
            else:
                # else table has data, then drop last 7 days data
                real_start = max((start, pd.to_datetime(real_start) - pd.Timedelta(days=7)))
                self.get_logger().debug(f'`max_dt` is not None, run from {real_start:%Y-%m-%d}.')
                session.query(IndexDerivativePrice).filter(
                    IndexDerivativePrice.benchmark_code.in_([f'{self.prefix}{s}{g}' for s, g in product('SMB', 'GNV')]),
                    IndexDerivativePrice.trade_dt > real_start
                ).delete(synchronize_session='fetch')

            month_end = max((t for t in get_dates(FreqEnum.M) if t <= real_start))
            ff3_close = pd.DataFrame(
                session.query(
                    IndexDerivativePrice.benchmark_code,
                    IndexDerivativePrice.close_,
                ).filter(
                    IndexDerivativePrice.trade_dt == month_end
                ).all(),
                columns=['benchmark_code', 'close_']
            ).set_index('benchmark_code').squeeze()

        self.io.localized_snapshot(month_end, if_exist=1)
        factor_val = self.io.fetch_snapshot(month_end)

        stock_close = self.get_adj_close(month_end)
        trade_dates = (t for t in get_dates(FreqEnum.D) if real_start < t <= self.last_td_date)

        for dt in trade_dates:
            self.get_logger().debug(f'run at {dt:%Y-%m-%d}.')
            cur_close = self.get_adj_close(dt)
            factor = factor_val.assign(ret=cur_close.div(stock_close) - 1)
            cum_ret = factor.groupby('label').apply(lambda df: df['ret'].dot(df['capt']) / df['capt'].sum())
            nav = ff3_close.mul(cum_ret.add_prefix(self.prefix).add(1)).round(6)
            self.bulk_insert(
                records=[dict(benchmark_code=k, trade_dt=dt, close_=v) for k, v in nav.items()],
                model=IndexDerivativePrice, msg=f'{dt:%Y-%m-%d}'
            )

            if dt in get_dates(FreqEnum.M):
                self.get_logger().debug(f'localized factor and close at {dt:%Y-%m-%d}.')
                month_end = dt
                self.io.localized_snapshot(dt, if_exist=1)
                factor_val = self.io.fetch_snapshot(dt)
                stock_close = cur_close.copy()
                ff3_close = nav.copy()

    @staticmethod
    def get_adj_close(dt):
        price = get_price(AssetEnum.STOCK, start=dt, end=dt, fields=['close_', 'adj_factor']).set_index('wind_code')
        return price['close_'].mul(price['adj_factor'])


if __name__ == '__main__':
    create_all_table()
    StockFF3Factor().run()
