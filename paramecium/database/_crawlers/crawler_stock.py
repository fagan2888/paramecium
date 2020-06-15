# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 14:40
@Author: Sue Zhu
"""
from itertools import product

import pandas as pd
import sqlalchemy as sa

from .._postgres import get_session
from ..comment import get_last_td, get_dates
from ..pg_models import stock, others
from ._base import CrawlerJob
from ... import const


class AShareDescription(CrawlerJob):
    """
    Crawling stock description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=25
    """

    def run(self, *args, **kwargs):
        model = stock.AShareDescription
        stock_info = pd.concat((
            self.get_tushare_data(api_name='stock_basic', exchange=exchange) for exchange in ('SSE', 'SZSE')
        )).filter(model.__dict__.keys(), axis=1)
        stock_info.loc[lambda df: df['list_dt'].notnull() & df['delist_dt'].isnull(), 'delist_dt'] = pd.Timestamp.max

        self.insert_data(records=stock_info, model=model, ukeys=model.get_primary_key())


class _CrawlerEOD(CrawlerJob):

    @property
    def model(self):
        return stock.AShareEODPrice

    def get_eod_data(self, **func_kwargs):
        return NotImplementedError

    def run(self, *args, **kwargs):
        with get_session() as session:
            max_dt = pd.to_datetime(session.query(
                sa.func.max(self.model.trade_dt).label('max_dt')
            ).one()[0]) - pd.Timedelta(days=5)
            if max_dt is pd.NaT:
                max_dt = pd.Timestamp('1990-01-01')

        trade_dates = [i for i in get_dates('D') if max_dt < i <= get_last_td()]
        price = pd.DataFrame()
        for i, dt in enumerate(trade_dates):
            # in case of data limit out.
            try:
                price = pd.concat((price, self.get_eod_data(trade_date=f'{dt:%Y%m%d}')), axis=0)
            except Exception as e:
                self.get_logger().error(f'error happens when run {repr(e)}')
                break

            if price.shape[0] > 10000:
                self.insert_data(records=price, model=self.model, msg=f'{i / len(trade_dates) * 100:.2f}%')
                price = pd.DataFrame()

        self.insert_data(records=price, model=self.model, msg='100%')
        self.clean_duplicates(self.model, [self.model.wind_code, self.model.trade_dt])


class ASharePrice(_CrawlerEOD):
    """
    Crawling stock end-of-date price from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=27
    """

    @property
    def model(self):
        return stock.AShareEODPrice

    def get_eod_data(self, **func_kwargs):
        self.get_logger().info(f'getting data from tushare: {func_kwargs}.')
        price = self.get_tushare_data(api_name='daily', **func_kwargs).filter(self.model.__dict__.keys(), axis=1)
        price.loc[:, 'trade_status'] = price['trade_status'].map({
            **const.TradeStatus.items(), '停牌': 0, '交易': -1, '待核查': -2,
        }).fillna(-2).astype(int)
        return price


class AShareEODDerivativeIndicator(_CrawlerEOD):
    """
    Crawling stock end-of-date price from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=32
    """

    @property
    def model(self):
        return stock.AShareEODDerivativeIndicator

    def get_eod_data(self, **func_kwargs):
        self.get_logger().info(f'getting data from tushare: {func_kwargs}.')
        price = super().get_tushare_data(api_name='daily_basic', **func_kwargs)
        return price.filter(self.model.__dict__.keys(), axis=1)


class AShareSuspend(CrawlerJob):
    """
    Crawling stock suspend info from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=31
    """

    def get_tushare_data(self, **func_kwargs):
        data = super().get_tushare_data(api_name='suspend', **func_kwargs)
        data.loc[lambda df: df['suspend_type'].eq(444003000), 'resume_date'] = pd.Timestamp.max
        return data

    def run(self, env='prod', *args, **kwargs):
        super().run(env, *args, **kwargs)
        model = stock.AShareSuspend

        with get_session() as session:
            # select stocks still on
            max_dt = pd.to_datetime(session.query(
                sa.func.max(model.suspend_date).label('max_dt')
            ).one()[0]) - pd.Timedelta(days=5)

            if max_dt is pd.NaT:
                # data do not exist, download by stock code
                desc = stock.AShareDescription
                stock_list = pd.DataFrame(session.query(desc.wind_code, desc.list_dt).all()).dropna()
                query_params = (dict(ts_code=code) for code in stock_list['wind_code'])
            else:
                # data exist, download by date
                query_params = ({key: f'{dt:%Y%m%d}'} for key, dt in product(
                    ('suspend_date', 'resume_date'),
                    (i for i in get_dates('D') if max_dt < i <= get_last_td())
                ))

        for q in query_params:
            data = self.get_tushare_data(api_name='suspend', **q)
            data.loc[lambda df: df['suspend_type'].eq(444003000), 'resume_date'] = pd.Timestamp.max
            self.insert_data(records=data, model=model)

        self.clean_duplicates(model, model.uk.columns)


class AShareIndustry(CrawlerJob):

    @property
    def enum_tb(self):
        return others.EnumIndustryCode

    def run(self, *args, **kwargs):
        for code, data in (*self.get_zz_industry(),):
            self.insert_data(data, msg=code, model=stock.AShareSector, ukeys=stock.AShareSector.uk_.columns)

    def get_zz_industry(self):
        with get_session() as session:
            industry_codes = session.query(self.enum_tb.code).filter(
                self.enum_tb.level_num == 3,
                sa.func.substr(self.enum_tb.code, 1, 2) == const.SectorEnum.SEC_ZZ.value
            ).all()

        for (code,) in industry_codes:
            data = self.get_tushare_data(
                api_name='index_member_zz', index_code=code
            ).drop('is_new', axis=1, errors='ignore').fillna({'remove_dt': pd.Timestamp.max})
            yield code, data

    def get_sw_industry(self):
        # TODO: not exist in prod, still have problem in dev server.
        with get_session() as session:
            industry_codes = session.query(
                self.enum_tb.code,
                self.enum_tb.memo,
            ).filter(
                self.enum_tb.level_num == 4,
                sa.func.substr(self.enum_tb.code, 1, 2) == const.SectorEnum.SEC_SW.value
            ).all()

        for code, idx in industry_codes:
            dt_cols = ['in_date', 'out_date']
            data = self.get_tushare_data(
                api_name='sw_index_member', index_code=idx, date_cols=dt_cols,
                col_mapping={'con_ts_code': 'wind_code', 'in_date': 'entry_dt', 'out_date': 'remove_dt'},
                fields=['con_ts_code', *dt_cols]
            ).fillna({'remove_dt': pd.Timestamp.max}).assign(sector_code=code)
            yield code, data

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
