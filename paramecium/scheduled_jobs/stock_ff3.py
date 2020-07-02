# -*- coding: utf-8 -*-
"""
@Time: 2020/6/1 10:50
@Author: Sue Zhu
"""
from itertools import product

import pandas as pd
import sqlalchemy as sa

from ._base import *
from ..const import FreqEnum, AssetEnum
from ..database import get_last_td, get_dates, FactorDBTool, get_price
from ..database.pg_models import index
from ..factor_pool import stock_classic


class StockFF3Factor(BaseJob):
    """
    Localized Stock Fama-French Data
    """
    meta_args = (
        {'type': 'string', 'description': 'date string that can be translate by pd.to_datetime, default is None'},
        {'type': 'string', 'description': 'date string that can be translate by pd.to_datetime, default is None'}
    )
    prefix = 'ff3_'

    def __init__(self, job_id=None, execution_id=None):
        super().__init__(job_id, execution_id)
        self.ff3 = stock_classic.FamaFrench()
        self.io = FactorDBTool(self.ff3)
        self.codes = [f'{self.prefix}{n}' for n in (''.join(m) for m in product('smb', 'gnv'))]
        self.names = [f'{sn}盘{gn}指数' for sn, gn in product('小中大', ('成长', '平衡', '价值'))]

    def run(self, start=None, end=None, *args, **kwargs):
        # make sure start is not None
        if end is None:
            end = get_last_td()

        # locate real start date
        with get_session() as session:
            self.get_logger().debug('query max date')
            real_start, = session.query(
                sa.func.max(index.DerivativePrice.trade_dt),
            ).filter(
                index.DerivativePrice.wind_code == self.codes[0]
            ).one()
        if real_start is None:
            # table is empty, so set first nav as base point.
            real_start = self.ff3.start_date
            self.get_logger().debug('`max_dt` is None, run from start.')
            self.insert_data(
                records=({
                    'wind_code': c, 'benchmark_name': n,
                    'base_date': real_start, 'base_point': 1e3,
                    'updated_at': sa.func.current_timestamp()
                } for c, n in zip(self.codes, self.names)),
                model=index.DerivativeDesc, ukeys=index.DerivativeDesc.get_primary_key(),
                msg='benchmark code into description table'
            )
            self.insert_data(
                records=({'wind_code': c, 'trade_dt': real_start, 'close_': 1e3} for c in self.codes),
                model=index.DerivativePrice,
            )
        else:
            # else table has data, then drop last 7 days data
            real_start = max((
                pd.Timestamp(start) if start is not None else self.ff3.start_date,
                pd.to_datetime(real_start) - pd.Timedelta(days=7),
            ))
            self.get_logger().debug(f'`max_dt` is not None, run from {real_start:%Y-%m-%d}.')
            with get_session() as session:
                session.query(index.DerivativePrice).filter(
                    index.DerivativePrice.wind_code.in_(self.codes),
                    index.DerivativePrice.trade_dt > real_start
                ).delete(synchronize_session='fetch')

        month_end = max((t for t in get_dates(FreqEnum.M) if t <= real_start))
        if real_start == month_end:
            self.io.localized_snapshot(month_end, if_exist=1)
        factor_val = self.io.fetch_snapshot(month_end)
        stock_close = self.get_stock_close(month_end)

        with get_session() as session:
            ff3_close = pd.DataFrame(
                session.query(
                    index.DerivativePrice.wind_code,
                    index.DerivativePrice.close_,
                ).filter(
                    index.DerivativePrice.trade_dt == month_end,
                    index.DerivativePrice.wind_code.in_(self.codes),
                ).all(),
                columns=['wind_code', 'close_']
            ).set_index('wind_code').squeeze()

        dates = (t for t in get_dates(FreqEnum.D) if real_start < t <= pd.Timestamp(end if end else get_last_td()))
        for dt in dates:
            self.get_logger().debug(f'run at {dt:%Y-%m-%d}.')
            cur_close = self.get_stock_close(dt)
            factor = factor_val.assign(ret=cur_close.div(stock_close) - 1)
            cum_ret = factor.groupby('label').apply(lambda df: df['ret'].dot(df['capt']) / df['capt'].sum())
            nav = ff3_close.mul(cum_ret.rename(index=lambda k: f'{self.prefix}{k.lower()}').add(1)).round(6)
            self.insert_data(
                records=[{'wind_code': k, 'trade_dt': dt, 'close_': v} for k, v in nav.items()],
                model=index.DerivativePrice, msg=f'{dt:%Y-%m-%d}'
            )

            if dt in get_dates(FreqEnum.M):
                self.get_logger().debug(f'localized factor and close at {dt:%Y-%m-%d}.')
                month_end = dt
                self.io.localized_snapshot(dt, if_exist=1)
                factor_val = self.io.fetch_snapshot(dt)
                stock_close = cur_close.copy()
                ff3_close = nav.copy()

    @staticmethod
    def get_stock_close(dt):
        price = get_price(AssetEnum.STOCK, start=dt, end=dt).set_index('wind_code')
        return price['close_'].mul(price['adj_factor'])
