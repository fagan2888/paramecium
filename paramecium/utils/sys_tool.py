# -*- coding: utf-8 -*-
"""
@Time: 2020/5/28 15:49
@Author: Sue Zhu
"""
import socket


def get_host_ip():
    """
    查询本机ip地址
    :reference: https://www.cnblogs.com/z-x-y/p/9529930.html
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip
