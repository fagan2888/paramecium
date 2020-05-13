# -*- coding: utf-8 -*-
"""
@Time: 2019/8/4 17:09
@Author: Sue Zhu

@Usage:
    Mainly use `scipy.optimize.minimize`.
    https://docs.scipy.org/doc/scipy-0.18.1/reference/generated/scipy.optimize.minimize.html
"""


def portfolio_optimizer(
        date, securities, target, constraints,
        bounds=(), default_port_weight_range=[0.0, 1.0], ftol=1e-9, return_none_if_fail='ignore'):
    """
    :param date: 优化发生的日期，请注意未来函数
    :param securities: 股票代码列表
    :param target: 优化目标函数，只能选择一个，目标函数详见下方
    :param constraints: 限制函数，用以对组合总权重进行限制，可设置一个或多个相同/不同类别的函数，限制函数详见下方
    :param bounds: 边界函数，用以对组合中单标的权重进行限制，可设置一个或多个相同/不同类别的函数，边界函数详见下方。
        默认为 Bound(0., 1.)；如果有多个 bound，则一只股票的权重下限取所有 Bound 的最大值，上限取所有 Bound 的最小值
    :param default_port_weight_range: 长度为2的列表，默认的组合权重之和的范围，默认值为 [0.0, 1.0]。如果constraints中没有
        WeightConstraint 或 WeightEqualConstraint 限制，则会添加 WeightConstraint(low=default_port_weight_range[0],
        high=default_port_weight_range[1]) 到 constraints列表中。
    :param ftol: 默认 1e-9，优化函数触发结束的函数值。当求解结果精度不够时可以适当降低，当求解时间过长时可以适当提高
    :param if_fail: str, 默认为'ignore'，此时如果优化失败返回 None，否则raise error.

    :return: weight(np.array)
    """
    pass
