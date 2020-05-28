# -*- coding: utf-8 -*-
"""
@Time: 2020/2/16 11:17
@Author: Sue Zhu
"""
import re

import numpy as np


def chunk(iterable, chunk_size):
    """ Generate sequences of elements from `iterable` with `chunk_size`"""
    records = []
    for i, record in enumerate(iterable, start=1):
        records.append(record)
        if i % chunk_size == 0:
            yield records
            records.clear()

    if records:
        yield records


# ===================== Math ============================================
def generate_exp_weights(half_life, n_weight):
    """ Generate `n` exponentially weights with `half_life` """
    exp_weight = np.array([1 / (0.5 ** (1 / half_life) ** i) for i in range(n_weight, 0, -1)])
    exp_weight /= exp_weight.sum()
    return exp_weight


# ===================== String ============================================
def camel2snake(strings):
    # ref: https://stackoverflow.com/a/1176023
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', strings)
    return re.sub('([a-zO-9])([A-Z])', r'\l_\2', s1).lower()


def capital_str():
    return (chr(i) for i in range(65, 91))
