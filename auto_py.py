# coding:utf-8

import pyautogui as ag
import numpy as np
import pandas as pd
from time import sleep

x, y = ag.size()

# 保护措施，避免失控
ag.FAILSAFE = True
# 为所有的PyAutoGUI函数增加延迟。默认延迟时间是0.1秒。
ag.PAUSE = 0.5

while pd.Timestamp.now()<pd.Timestamp(f'{pd.Timestamp.now():%Y-%m-%d} 23:30:00'):
	# while True:
	a, b = np.random.uniform(low=0.15, high=0.85, size=2)
	ag.moveTo(a*x, b*y, 1.5)
	ag.click()
	sleep(np.abs(np.random.randn()*3))
