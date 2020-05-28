# -*- coding:utf-8 -*-
"""
Created at 2019/6/23 by Sue.

Usage:
    
"""
import logging
import sys

logger = logging.getLogger()
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
