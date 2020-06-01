# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 16:11
@Author: Sue Zhu
"""
import logging

import pandas as pd

from paramecium.database.trade_calendar import TradeCalendar as CalModel
from paramecium.utils.date_tool import expand_calendar
from paramecium.scheduler_.jobs._localizer import TushareCrawlerJob

logger = logging.getLogger(__name__)


class CalendarCrawler(TushareCrawlerJob):
    """
    Crawler trade calendar from tushare
    """
    meta_args = (
        # pre_truncate:
        {'type': 'int', 'description': '0 or 1 as bool, default `1`'},
    )
    meta_args_example = '[1]'

    def run(self, pre_truncate=1, *args, **kwargs):
        if pre_truncate:
            name = CalModel.__tablename__
            with self.get_session() as session:
                self.get_logger().info(f'truncate table {name:s}.')
                session.execute(f'truncate table {name:s};')

        self.get_logger().info('Getting data from tushare')
        dates = self.get_tushare_data(api_name='trade_cal', exchange='SZSE', start_date='19000101')['trade_date']
        cal_df = expand_calendar(pd.to_datetime(dates, format='%Y%m%d'))
        cal_df.index.name = 'trade_dt'

        self.get_logger().info('upsert data')
        self.upsert_data(records=cal_df.reset_index(), model=CalModel, ukeys=[CalModel.trade_dt])


if __name__ == '__main__':
    CalendarCrawler().run(1)
