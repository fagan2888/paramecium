# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 16:11
@Author: Sue Zhu
"""
import logging

import pandas as pd
from bs4 import BeautifulSoup

from paramecium.database._postgres.cal_ import TradeCalendar
from paramecium.scheduler_.jobs._localizer import TushareCrawlerJob, WebCrawlerJob
from paramecium.utils.date_tool import expand_calendar

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
            name = TradeCalendar.__tablename__
            with self.get_session() as session:
                self.get_logger().info(f'truncate table {name:s}.')
                session.execute(f'truncate table {name:s};')

        self.get_logger().info('Getting data from tushare')
        dates = self.get_tushare_data(api_name='trade_cal', exchange='SZSE', start_date='19000101')['trade_date']
        cal_df = expand_calendar(pd.to_datetime(dates, format='%Y%m%d'))
        cal_df.index.name = 'trade_dt'

        self.get_logger().info('upsert data')
        self.upsert_data(records=cal_df.reset_index(), model=TradeCalendar, ukeys=[TradeCalendar.trade_dt])


class RateBaselineCrawler(WebCrawlerJob):
    """
    基准利率调整
    http://data.eastmoney.com/cjsj/yhll.html
    """

    def run(self, *args, **kwargs):
        respond = self.request('http://datainterface.eastmoney.com/EM_DataCenter/XML.aspx?type=GJZB&style=ZGZB&mkt=13')
        if respond.status_code != 200:
            raise ConnectionError(f"Fail to fetch data for {self.__class__.__name__}")

        bs = BeautifulSoup(respond.text, features="lxml")
        data = pd.DataFrame(
            data=([[*pd.to_datetime(self.html2list(bs.find('series')), format='%y-%m-%d')]]
                  + [self.html2list(g) for g in bs.find_all('graph')]),
            index=['change_dt', 'loan_rate', 'save_rate'],
        ).T.astype({'loan_rate': float, 'save_rate': float})
        self.upsert_data(data, macro.InterestRate, macro.InterestRate.get_primary_key())

    @staticmethod
    def html2list(html_series):
        return [v.text for v in html_series.find_all('value')]


if __name__ == '__main__':
    from paramecium.database._postgres import create_all_table, macro

    create_all_table()
    # CalendarCrawler().run(1)
    RateBaselineCrawler().run()
