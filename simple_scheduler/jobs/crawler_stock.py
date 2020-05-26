# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 14:40
@Author: Sue Zhu
"""

from paramecium.database import model_stock_org, TradeCalendar

from simple_scheduler.jobs._crawler import *

logger = logging.getLogger(__name__)


class AShareDescription(TushareCrawlerJob):
    """
    Crawling stock description from tushare
    """

    _COL = {
        'ts_code': 'wind_code',
        'name': 'short_name',
        'comp_name': 'comp_name',
        'comp_name_en': 'comp_name_en',
        'isin_code': 'isin_code',
        'exchange': 'exchange',
        'list_board': 'list_board',
        'list_date': 'list_dt',
        'delist_date': 'delist_dt',
        'crncy_code': 'currency',
        'pinyin': 'pinyin',
        'is_shsc': 'is_shsc',
        'comp_code': 'comp_code',
    }

    def run(self, *args, **kwargs):
        stock_info = pd.concat((
            self.get_tushare_data(
                api_name='stock_basic',
                exchange=exchange, fields=','.join(self._COL.keys())
            ) for exchange in ('SSE', 'SZSE')
        )).rename(columns=self._COL).fillna(np.nan)
        stock_info.loc[:, 'list_dt'] = pd.to_datetime(stock_info['list_dt'])
        stock_info.loc[:, 'delist_dt'] = pd.to_datetime(stock_info['delist_dt'])
        stock_info.loc[lambda df: df['list_dt'].notnull()&df['delist_dt'].isnull(), 'delist_dt'] = pd.Timestamp.max
        stock_info.loc[stock_info['list_dt'].isnull(), 'delist_dt'] = pd.NaT

        model = model_stock_org.AShareDescription
        self.upsert_data(records=stock_info, model=model, ukeys=[model.wind_code])


class ASharePrice(TushareCrawlerJob):
    """
    Crawling stock end-of-date price from tushare
    """

    def run(self, *args, **kwargs):
        model = model_stock_org.AShareEODPrice
        max_dt = pd.to_datetime(self.sa_session.query(sa.func.max(model.trade_dt).label('max_dt')).one()[0])
        if not max_dt:
            max_dt = pd.Timestamp('1990-01-01')

        trade_dates = [i for row in self.sa_session.query(
            TradeCalendar.trade_dt
        ).filter(
            TradeCalendar.is_d == 1,
            TradeCalendar.trade_dt > max_dt - pd.Timedelta(days=5),
            TradeCalendar.trade_dt < self.last_td_date,
        ).all() for i in row]
        mapping = {
            'ts_code': 'wind_code',
            'trade_date': 'trade_dt',
            'open': 'open_',
            'high': 'high_',
            'low': 'low_',
            'close': 'close_',
            'volume': 'volume_',
            'amount': 'amount_',
        }
        price = pd.DataFrame()
        for i, dt in enumerate(trade_dates):
            try:
                self.logger.info(f'getting {dt:%Y-%m-%d} nav from tushare')
                price = pd.concat((
                    price,
                    self.get_tushare_data(
                        'daily',
                        date_cols=['trade_date'],
                        org_cols=[*mapping.keys(), 'adj_factor', 'avg_price', 'trade_status'],
                        col_mapping=mapping,
                        trade_date=f'{dt:%Y%m%d}'
                    )
                ), axis=0)
            except Exception as e:
                self.logger.error(f'error happends when run {repr(e)}')
                break

            if price.shape[0] > 10000:
                self.upsert_data(
                    records=price, model=model,
                    ukeys=[model.wind_code, model.trade_dt],
                    msg=f'stock price {i / len(trade_dates) * 100:.2f}%',
                )
                price = pd.DataFrame()

        self.upsert_data(
            records=price, model=model,
            ukeys=[model.wind_code, model.trade_dt],
            msg='stock price',
        )


# class AShareAnnouncement(WebCrawlerJob):
#     """
#     Crawler stock announcement from `eastmoney.com`
#     Note: announcement address should be like this
#     http://data.eastmoney.com/notices/detail/{symbol}/{info_code},{random_code}.html
#     """
#     meta_args = (
#         # is_update
#         {'type': 'int', 'description': '1: update mode, 0: replace mode'},
#         # argument2
#         {'type': 'string', 'description': 'Second argument'}
#     )
#     meta_args_example = '[1]'
#
#     def __init__(self, job_id, execution_id):
#         super().__init__(job_id, execution_id)
#         self.security = {}
#         self.board = {}
#         self.security = {}
#
#     @staticmethod
#     def url(var_name, dt, page=1):
#         return (f'http://data.eastmoney.com/notices/getdata.ashx?StockCode=&FirstNodeType=0&CodeType=1&'
#                 f'PageIndex={page}&PageSize=100&jsObj={var_name}&SecNodeType=0&Time={dt:%Y-%m-%d}')
#
#     @staticmethod
#     def format_data(raw_dict):
#         security = raw_dict['CDSY_SECUCODES']
#         return pd.Series({
#             'affect_dt': pd.Timestamp(raw_dict['ENDDATE']).date(),
#             'ann_dt': pd.Timestamp(raw_dict['NOTICEDATE']).date(),
#             'title': raw_dict['NOTICETITLE'],
#             'em_info_code': raw_dict['INFOCODE'],
#             'em_table_id': raw_dict['TABLEID'],
#         })
#
#     def run(self, start_date='', *args, **kwargs):
#         if start_date:
#             start_date = pd.Timestamp(start_date)
#         else:
#             start_date = pd.Timestamp.now() - pd.Timedelta(days=1)
#
#         for dt in pd.date_range(start_date, pd.Timestamp.now()):
#             var_name = self.random_str(8)
#             respond = self.request('GET', self.url(var_name, dt))
#             if respond.status_code == 200:
#                 respond_data = json.loads(respond.text.replace(f'var {var_name} = ', '', count=1)[:-1])


if __name__ == '__main__':
    AShareDescription().run()
    ASharePrice().run()
