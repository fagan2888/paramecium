# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 10:51
@Author: Sue Zhu
"""
import pandas as pd


def expand_calendar(trade_dates):
    """
    根据交易日形成日历对象，包含不同freq的判断
    :param trade_dates: list-like series of datetime-like data
    :return: pd.DataFrame, index为日期, columns 为 is_{freq:w/m/q/y}
    """
    _CAL_GROUP = {
        'is_w': ('week', 'year'),
        'is_m': ('month', 'year'),
        'is_q': ('quarter', 'year'),
        'is_y': ('year',),
    }
    cal = pd.DataFrame(0, columns=_CAL_GROUP.keys(), index=trade_dates).assign(is_d=1)
    for freq_str, cols in _CAL_GROUP.items():
        cal.loc[cal.groupby([getattr(cal.index, o) for o in cols]).tail(1).index, freq_str] = 1
    return cal.resample('D').asfreq().fillna(0).astype(int)