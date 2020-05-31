# -*- coding:utf-8 -*-
"""
Created at 2019/6/23 by Sue.

Usage:
    
"""
import logging
import sys

logger = logging.getLogger()
logging.getLogger().setLevel(logging.DEBUG)

if 'StreamHandler' not in (c.__class__.__name__ for c in logger.handlers):
    bash_line_handler = logging.StreamHandler(sys.stdout)
    bash_line_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(bash_line_handler)
