# -*- coding: utf-8 -*-
"""
@Time: 2020/7/2 13:03
@Author: Sue Zhu
"""
import numpy as np
import pandas as pd
from scipy.optimize import minimize

from ..const import AssetEnum
from ..database import FundUniverse, FactorDBTool
from ..exc import DataExistError
from ..factor_pool import fund_stats
from ..interface import AbstractFactor
from ..utils import transformer as tf






class AllocationPureBond(AbstractFactor):
    asset_type = AssetEnum.CMF
    start_date = fund_stats.FundRegBond5.start_date

    def __init__(self, idx_win, bk_win, freq='W'):
        self._raw = fund_stats.FundRegBond5(bk_win, freq)
        self.universe = FundUniverse(include_=('2001010301000000', '2001010303000000'), size_=1)
        self._io = FactorDBTool(self._raw)
        self.idx_win = idx_win

    @property
    def index_ret(self):
        return self._raw.index_ret

    @property
    def field_types(self):
        return {'mix_factor': float}

    @property
    def name(self):
        return f'pure_bond_allocation{self._raw.name.split("_")[-1]}_{self.idx_win}'

    def compute(self, dt):
        try:
            self._io.localized_snapshot(dt, if_exist=0)
        except DataExistError:
            pass

        raw_factor = self._io.fetch_snapshot(dt).filter(self.universe.get_instruments(dt), axis=0)
        for trans in (tf.OutlierMAD(), tf.ScaleNormalize()):
            raw_factor.loc[:] = trans.fit_transform(raw_factor.values)

        constrains = (
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},
            {'type': 'ineq', 'fun': lambda w: w},
        )

        cur_factor_ret = self.index_ret.loc[:dt, ['market', 'credit', 'default_']].tail(self.idx_win)
        ret_mean = cur_factor_ret.add(1).prod().pow(250 / cur_factor_ret.shape[0]).sub(1)

        sigma_ann = cur_factor_ret.cov() * 250
        n_rp = sigma_ann.shape[0]

        opt_mvo = minimize(
            lambda w: -(w @ ret_mean) / np.sqrt(w @ sigma_ann @ w.T),
            np.ones((1, n_rp)) / n_rp,
            method='SLSQP',
            constraints=constrains,
        )
        cur_factor_ret = pd.DataFrame(
            dict(convert=self.index_ret['convert'], bond=cur_factor_ret @ opt_mvo.x)
        ).dropna()

        sigma_rp = cur_factor_ret.cov() * 250
        opt_rp = minimize(
            lambda w: np.std(w.T * (sigma_rp @ w.T) / np.sqrt(w @ sigma_rp @ w.T).squeeze()),
            np.ones((1, 2)) / 2,
            method='SLSQP',
            constraints=constrains,
        )
        weight = pd.Series(
            [*(opt_mvo.x * opt_rp.x[1]), 0, opt_rp.x[0]],
            index=['market', 'credit', 'default_', 'cmoney', 'convert']
        ).round(6)
        new_factor = raw_factor.mul(weight).sum(axis=1)
        return new_factor.to_frame('mix_factor')
