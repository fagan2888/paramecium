# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 16:11
@Author: Sue Zhu
"""
import logging

import pandas as pd

from paramecium.database import TradeCalendar as CalModel
from paramecium.utils import expand_calendar
from simple_scheduler.jobs._crawler import TushareCrawlerJob

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
            session = self.sa_session
            try:
                session.execute(f'truncate table {CalModel.__tablename__};')
            except Exception as e:
                logger.warning(f'{self.__class__.__name__:s} fail to truncate table before insert '
                               f'with exception {repr(e)}.')
                session.rollback()
            else:
                session.commit()

        self.logger.info('Getting data from tushare')
        dates = self.get_tushare_data(api_name='trade_cal', exchange='SZSE', start_date='19000101')['trade_date']
        cal_df = expand_calendar(pd.to_datetime(dates, format='%Y%m%d'))
        cal_df.index.name = 'trade_dt'

        self.logger.info('upsert data')
        self.upsert_data(records=cal_df.reset_index(), model=CalModel, ukeys=[CalModel.trade_dt])


if __name__ == '__main__':
    CalendarCrawler().run(1)
