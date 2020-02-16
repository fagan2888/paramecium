# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 11:17
@Author:  MUYUE1
"""
import re

import numpy as np

# ===================== Math ============================================
def generate_exp_weight(half_life, n_weight):
    '''
    Generate `n` exponentially weight with `half_life`
    :param half_life:
    :param n_weight:
    :return:
    '''
    exp_weight = np.array([1 / (0.5 ** (1 / half_life) ** i) for i in range(1, n_weight + 1)])
    exp_weight /= exp_weight.sum()
    return exp_weight


# ===================== String ============================================
def camel2snake(strings):
    # ref: https://stackoverflow.com/a/1176023
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', strings)
    return re.sub('([a-zO-9])([A-Z])', r'\l_\2', s1).lower()


def capital_str():
    return (chr(i) for i in range(65, 91))