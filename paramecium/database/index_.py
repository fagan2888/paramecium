# -*- coding: utf-8 -*-
"""
@Time: 2020/6/9 11:05
@Author: Sue Zhu
"""
__all__ = ['calc_market_factor', 'calc_timing_factor', 'get_index_ff3', 'get_index_bond5']

import pandas as pd

from .comment import get_price, get_risk_free_rates, get_dates, resampler
from .. import const


def _get_index_price(code):
    price = get_price(const.AssetEnum.INDEX, code=code)
    return price.set_index('trade_dt').loc[lambda ser: ~ser.index.duplicated(), 'close_']


def _resample_ret(price, freq):
    return price.rename(index=resampler(freq)).loc[lambda ser: ~ser.index.duplicated()].pct_change(1)


def calc_market_factor(code_or_price, calc_freq=const.FreqEnum.W):
    """
    calculate market factor without risk-free rate

    comment factor code for markets:
        - A Share: h00985.CSI 中证全指; h00300.CSI; h00905.CSI
        - Bond: CBA00301.CS; h11001.CSI
        - CMoney: h11025.CSI; 885009.WI

    :param code_or_price: str or series or dict
    :param calc_freq:
    :return:
    """
    if isinstance(code_or_price, str):
        code_or_price = _get_index_price(code_or_price)
    elif isinstance(code_or_price, dict):
        code_or_price = pd.Series(code_or_price)

    return _resample_ret(code_or_price, calc_freq).sub(get_risk_free_rates('save', calc_freq)).dropna()


def _unit_gii_factor(df):
    """
    GII Timing Factor

    $ P_{mt} = \big[ \prod_{k \in month(t)}^t \max(1+r_{mk}, 1+r_{fk}) \big] - (1+r_{mt}) $
    $ r_t = \alpha + \beta r_{mt} + \gamma P_{mt} + \varepsilon_t $
    """
    market_ret, rf = df['market'], df['rf']
    prod_alpha = market_ret.where(market_ret > rf, rf).add(1).prod()
    prod_mkt = market_ret.add(1).prod()
    return prod_alpha - prod_mkt


def calc_timing_factor(code_or_price, method='gii', calc_freq=const.FreqEnum.W):
    if isinstance(code_or_price, str):
        code_or_price = _get_index_price(code_or_price)

    if method == 'gii':
        _FREQ_ORDER = list('DWMQY')
        base_freq = const.FreqEnum[_FREQ_ORDER[_FREQ_ORDER.index(calc_freq.name) - 1]]
        org_ret = pd.DataFrame({
            'market': _resample_ret(code_or_price, base_freq),
            'rf': get_risk_free_rates('save', base_freq),
            'label': get_dates(calc_freq).to_series()
        })
        org_ret.loc[:, 'label'] = org_ret['label'].ffill().bfill()
        timing = org_ret.dropna().groupby('label').apply(_unit_gii_factor)
        return timing

    elif method in ('hm', 'tm'):
        rf = get_risk_free_rates('save', calc_freq)
        market = _resample_ret(code_or_price, calc_freq)
        net_ret = market.sub(rf).dropna()
        if method == 'hm':
            return net_ret.where(net_ret > 0, 0)
        else:
            return net_ret ** 2

    else:
        raise KeyError(f"Unknown `method` {method}.")


def get_index_ff3(calc_freq=const.FreqEnum.W, timing=None):
    market_price = _get_index_price('h00985.CSI')

    box9_price = pd.DataFrame({f'{c}{v}': _get_index_price(f'ff3_{c}{v}') for c, v in zip('smb', 'gnv')})
    box9_ret = _resample_ret(box9_price, calc_freq).iloc[1:]

    filter_mean = lambda x: box9_ret.filter(regex=x, axis=1).mean(axis=1)
    factors = pd.DataFrame({
        'market': calc_market_factor(code_or_price=market_price, calc_freq=calc_freq),
        'smb': filter_mean('s.') - filter_mean('b.'),
        'hml': filter_mean('.v') - filter_mean('.g'),
    }).dropna()
    if timing:
        factors['timing'] = calc_timing_factor(market_price, calc_freq=calc_freq, method=timing)
    return factors


def get_index_bond5(calc_freq=const.FreqEnum.W):
    bond_market = _get_index_price('CBA00301.CS')
    credit_3a = _get_index_price('CBA04201.CS')
    high_yield = _get_index_price('CBA03801.CS')

    factors = pd.DataFrame({
        'market': calc_market_factor(code_or_price=bond_market, calc_freq=calc_freq),
        'credit': _resample_ret(credit_3a, calc_freq) - _resample_ret(high_yield, calc_freq),
        'default': _resample_ret(credit_3a, calc_freq) - _resample_ret(high_yield, calc_freq),
        'cmoney': calc_market_factor(code_or_price='h11025.CSI', calc_freq=calc_freq),
        'convert': calc_market_factor(code_or_price='h00906.CSI', calc_freq=calc_freq),
    }).dropna()
    return factors
