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
        self.codes = [f'{self.prefix}{n}' for n in (*(''.join(m) for m in product('smb', 'gnv')), 'smb', 'hml')]
        self.names = [
            *(f'{sn}盘{gn}指数' for sn, gn in product('小中大', ('成长', '平衡', '价值'))),
            *(f'FamaFrench{name}因子' for name in ('规模', '价值'))
        ]

    def run(self, start=None, end=None, *args, **kwargs):
        # make sure start is not None
        if start is None:
            start = self.ff3.start_date
        if end is None:
            end = self.last_td_date

        with self.get_session() as session:
            self.get_logger().debug('query max date')
            real_start, = session.query(
                sa.func.max(IndexDerivativePrice.trade_dt),
            ).filter(
                IndexDerivativePrice.benchmark_code == self.codes[0]
            ).one()

        if real_start is None:
            # table is empty, so set first nav as base point.
            real_start = self.ff3.start_date
            self.get_logger().debug('`max_dt` is None, run from start.')
            self.upsert_data(
                ({
                    'benchmark_code': c, 'benchmark_name': n,
                    'base_date': real_start, 'base_point': 1e3,
                    'updated_at': sa.func.current_timestamp()
                } for c, n in zip(self.codes, self.names)),
                IndexDerivativeDesc, ukeys=[IndexDerivativeDesc.benchmark_code],
                msg='benchmark code into description table'
            )
            self.bulk_insert(
                ({'benchmark_code': c, 'trade_dt': real_start, 'close_': 1e3} for c in self.codes),
                IndexDerivativePrice,
            )
        else:
            # else table has data, then drop last 7 days data
            real_start = max((
                start,
                pd.to_datetime(real_start) - pd.Timedelta(days=7)
            ))
            self.get_logger().debug(f'`max_dt` is not None, run from {real_start:%Y-%m-%d}.')
            with self.get_session() as session:
                session.query(IndexDerivativePrice).filter(
                    IndexDerivativePrice.benchmark_code.in_(self.codes),
                    IndexDerivativePrice.trade_dt > real_start
                ).delete(synchronize_session='fetch')

        month_end = max((t for t in get_dates(FreqEnum.M) if t <= real_start))
        if real_start == month_end:
            self.io.localized_snapshot(month_end, if_exist=1)
        factor_val = self.io.fetch_snapshot(month_end)
        stock_close = self.get_stock_close(month_end)
        ff3_close = self.get_index_close(self.codes[:-2], month_end)

        trade_dates = (t for t in get_dates(FreqEnum.D) if real_start < t <= pd.Timestamp(end))
        for dt in trade_dates:
            self.get_logger().debug(f'run at {dt:%Y-%m-%d}.')
            cur_close = self.get_stock_close(dt)
            factor = factor_val.assign(ret=cur_close.div(stock_close) - 1)
            cum_ret = factor.groupby('label').apply(lambda df: df['ret'].dot(df['capt']) / df['capt'].sum())
            nav = ff3_close.mul(cum_ret.rename(index=lambda k: f'{self.prefix}{k.lower()}').add(1)).round(6)
            self.bulk_insert(
                records=[{'benchmark_code': k, 'trade_dt': dt, 'close_': v} for k, v in nav.items()],
                model=IndexDerivativePrice, msg=f'{dt:%Y-%m-%d}'
            )

            if dt in get_dates(FreqEnum.M):
                self.get_logger().debug(f'localized factor and close at {dt:%Y-%m-%d}.')
                month_end = dt
                self.io.localized_snapshot(dt, if_exist=1)
                factor_val = self.io.fetch_snapshot(dt)
                stock_close = cur_close.copy()
                ff3_close = nav.copy()

        with self.get_session() as ss:
            cols = ('trade_dt', 'benchmark_code', 'close_')
            ret = pd.DataFrame(
                ss.query(
                    *(getattr(IndexDerivativePrice, c) for c in cols)
                ).filter(
                    IndexDerivativePrice.benchmark_code.in_(self.codes[:-2]),
                    IndexDerivativePrice.trade_dt >= real_start
                ).all()
            ).pivot(*cols).rename(index=pd.to_datetime).pct_change(1)
        filter_mean = lambda x: ret.filter(regex=f'{self.prefix}{x}', axis=1).mean(axis=1).fillna(0)

        combine = pd.DataFrame({
            f'{self.prefix}smb': filter_mean('s.') - filter_mean('b.'),
            f'{self.prefix}hml': filter_mean('.v') - filter_mean('.g'),
        }).add(1).cumprod().mul(self.get_index_close(codes=self.codes[-2:], dt=real_start))
        self.bulk_insert(
            records=[{'benchmark_code': c, 'trade_dt': t, 'close_': p} for (t, c), p in combine.stack().items()],
            model=IndexDerivativePrice, msg='whole smb and hml'
        )

    @staticmethod
    def get_stock_close(dt):
        price = get_price(AssetEnum.STOCK, start=dt, end=dt, fields=['close_', 'adj_factor']).set_index('wind_code')
        return price['close_'].mul(price['adj_factor'])

    def get_index_close(self, codes, dt):
        with self.get_session() as session:
            ff3_close = pd.DataFrame(
                session.query(
                    IndexDerivativePrice.benchmark_code,
                    IndexDerivativePrice.close_,
                ).filter(
                    IndexDerivativePrice.trade_dt == dt,
                    IndexDerivativePrice.benchmark_code.in_(codes),
                ).all(),
                columns=['benchmark_code', 'close_']
            ).set_index('benchmark_code').squeeze()
        return ff3_close


if __name__ == '__main__':
    create_all_table()
    StockFF3Factor().run(end='2020-05-15')
