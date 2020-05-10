# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 11:17
@Author:  MUYUE1
"""
import re

import numpy as np
from scipy import stats

# ===================== Math ============================================
def generate_exp_weights(half_life, n_weight):
    """ Generate `n` exponentially weights with `half_life` """
    exp_weight = np.array([1 / (0.5 ** (1 / half_life) ** i) for i in range(n_weight, 0, -1)])
    exp_weight /= exp_weight.sum()
    return exp_weight

def outlier_mad(raw_factor):
    # excess values - median absolute deviation (MAD)
    factor_median = np.nanmedian(raw_factor, axis=0)

    # in scipy 1.3+ can use scipy.stats.median_absolute_deviation for calculate mad
    mad = np.nanmedian(np.abs(raw_factor - factor_median))

    sigma = 1.4826 * mad
    upper_bound = factor_median+3*sigma
    lower_bound = factor_median-3*sigma
    new_factor = raw_factor.copy()
    new_factor.loc[raw_factor>upper_bound] = raw_factor.loc[raw_factor>upper_bound].rank(ascending=True, pct=True)*0.5*sigma+upper_bound
    new_factor.loc[raw_factor<lower_bound] = lower_bound - raw_factor.loc[raw_factor<lower_bound].rank(ascending=False, pct=True)*0.5*sigma
    return new_factor


# ===================== String ============================================
def camel2snake(strings):
    # ref: https://stackoverflow.com/a/1176023
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', strings)
    return re.sub('([a-zO-9])([A-Z])', r'\l_\2', s1).lower()


def capital_str():
    return (chr(i) for i in range(65, 91))
