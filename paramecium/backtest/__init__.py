# -*- coding: utf-8 -*-
"""
@Time: 2019/8/4 10:24
@Author: Sue Zhu
"""
import logging
from contextlib import contextmanager

from paramecium.configuration import get_data_config

_log = logging.getLogger(__name__)


@contextmanager
def get_ifind_api():
    """ 获取同花顺IFind API """
    from iFinDPy import THS_iFinDLogin, THS_iFinDLogout
    err_code = THS_iFinDLogin(**get_data_config('ifind'))
    if err_code == '2':
        _log.error("iFind API has an error!")
    else:
        _log.debug("Successful login IFind")
        yield err_code
    _log.debug("Try to logout IFind")
    THS_iFinDLogout()


@contextmanager
def get_wind_api():
    """ 登录Wind API """
    from WindPy import w
    w.start()  # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒
    if w.isconnected():  # 判断WindPy是否已经登录成功
        yield w
    else:
        _log.error("Wind API fail to be connected.")
    w.stop()