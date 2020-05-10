# -*- coding: utf-8 -*-
"""
@Time: 2019/8/11 18:18
@Author:  MUYUE1
"""
import abc


class AbstractMod(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def start_up(self, config):
        return NotImplementedError

    def tear_down(self, exception=None):
        pass
