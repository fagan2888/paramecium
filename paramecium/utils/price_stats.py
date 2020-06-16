# -*- coding: utf-8 -*-
"""
@Time: 2020/6/9 14:41
@Author: Sue Zhu

Utility functions use for portfolio return analysis.
All use numpy to speed up.
"""
from dataclasses import dataclass

import numpy as np


def cumulative_returns(returns):
    return np.nanprod(returns + 1, axis=0) - 1


def annual_returns(returns, mul=250):
    return (1 + cumulative_returns(returns)) ** (returns.shape[0] / mul) - 1


def annual_volatility(returns, mul=250):
    return np.nanstd(returns, ddof=1, axis=0) * np.sqrt(mul)


def max_draw_down(returns):
    cum_nav = np.cumprod(np.nan_to_num(returns) + 1, axis=0)
    high_water = np.maximum.accumulate(cum_nav, axis=0)
    draw_down = np.true_divide(cum_nav, high_water) - 1
    return np.min(draw_down, axis=0)


def up_side_risk(returns, rf=0, mul=250):
    up_side_ret = np.where(returns - rf > 0, returns - rf, 0)
    return np.nanstd(up_side_ret, ddof=1, axis=0) * np.sqrt(mul)


def down_side_risk(returns, rf=0, mul=250):
    down_side_ret = np.where(returns - rf < 0, returns - rf, 0)
    return np.nanstd(down_side_ret, ddof=1, axis=0) * np.sqrt(mul)


def sharpe_ratio(returns, rf=0, mul=250):
    return np.nan_to_num(
        annual_returns(returns - rf, mul) / annual_volatility(returns, mul),
        nan=np.nan, posinf=np.nan, neginf=np.nan
    )


def sortino_ratio(returns, rf=0, mul=250):
    return np.nan_to_num(
        annual_returns(returns - rf, mul) / down_side_risk(returns, rf, mul),
        nan=np.nan, posinf=np.nan, neginf=np.nan
    )


def calmar_ratio(returns, rf=0, mul=250):
    return np.nan_to_num(
        -1 * annual_returns(returns - rf, mul) / max_draw_down(returns),
        nan=np.nan, posinf=np.nan, neginf=np.nan
    )


def value_at_risk(returns, alpha=0.05):
    return np.nanpercentile(returns, q=alpha * 100, axis=0)


def conditional_var(returns, alpha=0.05):
    var = value_at_risk(returns, alpha)
    return np.nanmean(np.where(returns - var < 0, returns, np.nan), axis=0)


@dataclass
class RegressionResult(object):
    beta: np.array
    t_value: np.array
    r2: np.array


def regression(returns, factors, sample_weight=None):
    """
    Regression function directly use numpy function. maybe add a weight param later.
    :param returns: returns array with n sample and k portfolio.
    :param factors: returns array with n sample and k factor.
    :param w_diag: n*1 array
    :return:
    """
    if sample_weight is None:
        w_diag = np.eye(factors.shape[0])
    else:
        w_diag = np.diag(sample_weight)

    cov_inv = np.linalg.pinv(factors.T @ w_diag @ factors)
    beta = cov_inv @ factors.T @ w_diag @ returns
    rss = np.sum(w_diag @ np.square(returns - factors @ beta), axis=0, keepdims=True)
    n, k = factors.shape
    beta_stand_error = np.sqrt(rss.T @ cov_inv.diagonal().reshape((1, -1)) / (n - k))
    t_value = np.divide(beta.T, beta_stand_error)

    weight_true = np.average(returns, weights=w_diag.diagonal(), axis=0)
    tss = np.sum(w_diag @ np.square(returns - weight_true), axis=0, keepdims=True)
    r2_value = 1 - np.divide(rss, tss)
    return RegressionResult(beta=beta.T, t_value=t_value, r2=r2_value.T)
