# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 14:40
@Author: Sue Zhu
"""
from itertools import product

import numpy as np
import pandas as pd
import sqlalchemy as sa

from paramecium.const import TradeStatus, SectorEnum
from paramecium.database import stock_org, enum_code
from paramecium.scheduler_.jobs._crawler import TushareCrawlerJob


class AShareDescription(TushareCrawlerJob):
    """
    Crawling stock description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=25
    """

    _COL = {
        'ts_code': 'wind_code',
        'name': 'short_name',
        'list_date': 'list_dt',
        'delist_date': 'delist_dt',
        'crncy_code': 'currency',
    }

    def run(self, *args, **kwargs):
        stock_info = pd.concat((
            self.get_tushare_data(
                api_name='stock_basic', exchange=exchange,
                fields=[*self._COL.keys(), 'comp_name', 'comp_name_en', 'isin_code', 'exchange', 'list_board',
                        'pinyin', 'is_shsc', 'comp_code']
            ) for exchange in ('SSE', 'SZSE')
        )).rename(columns=self._COL).fillna(np.nan)
        stock_info.loc[:, 'list_dt'] = pd.to_datetime(stock_info['list_dt'])
        stock_info.loc[:, 'delist_dt'] = pd.to_datetime(stock_info['delist_dt'])
        stock_info.loc[lambda df: df['list_dt'].notnull() & df['delist_dt'].isnull(), 'delist_dt'] = pd.Timestamp.max
        stock_info.loc[stock_info['list_dt'].isnull(), 'delist_dt'] = pd.NaT

        model = stock_org.AShareDescription
        self.upsert_data(records=stock_info, model=model, ukeys=model.get_primary_key())


class _CrawlerEOD(TushareCrawlerJob):

    @property
    def model(self):
        return stock_org.AShareEODPrice

    def get_tushare_data_(self, **func_kwargs):
        return NotImplementedError

    def run(self, *args, **kwargs):
        with self.get_session() as session:
            max_dt = pd.to_datetime(session.query(
                sa.func.max(self.model.trade_dt).label('max_dt')
            ).one()[0]) - pd.Timedelta(days=5)

        if max_dt is pd.NaT:
            max_dt = pd.Timestamp('1990-01-01')

        last_dt = self.last_td_date
        trade_dates = [i for i in self.get_dates('D') if max_dt < i <= last_dt]
        price = pd.DataFrame()
        for i, dt in enumerate(trade_dates):
            try:
                # in case of data limit out.
                price = pd.concat((price, self.get_tushare_data_(trade_date=f'{dt:%Y%m%d}')), axis=0)
            except Exception as e:
                self.get_logger().error(f'error happends when run {repr(e)}')
                break

            if price.shape[0] > 10000:
                self.bulk_insert(records=price, model=self.model, msg=f'{i / len(trade_dates) * 100:.2f}%')
                price = pd.DataFrame()

        self.bulk_insert(records=price, model=self.model, msg='100%')
        self.clean_duplicates(self.model, [self.model.wind_code, self.model.trade_dt])


class ASharePrice(_CrawlerEOD):
    """
    Crawling stock end-of-date price from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=27
    """

    @property
    def model(self):
        return stock_org.AShareEODPrice

    def get_tushare_data_(self, **func_kwargs):
        self.get_logger().info(f'getting data from tushare: {func_kwargs}.')
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
        price = super().get_tushare_data(
            api_name='daily',
            date_cols=['trade_date'], col_mapping=mapping,
            fields=[*mapping.keys(), 'adj_factor', 'avg_price', 'trade_status'],
            **func_kwargs
        )
        price.loc[:, 'trade_status'] = price['trade_status'].map({
            **TradeStatus.items(), '停牌': 0, '交易': -1, '待核查': -2,
        }).fillna(-2).astype(int)
        return price


class AShareSuspend(TushareCrawlerJob):
    """
    Crawling stock suspend info from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=31
    """

    @property
    def model(self):
        return stock_org.AShareSuspend

    def get_tushare_data(self, **func_kwargs):
        data = super().get_tushare_data(
            api_name='suspend',
            date_cols=['suspend_date', 'resump_date'],
            col_mapping={
                'ts_code': 'wind_code',
                'resump_date': 'resume_date',
                'change_reason_type': 'reason_type',
            },
            **func_kwargs
        )
        data.loc[lambda df: df['suspend_type'].eq(444003000), 'resume_date'] = pd.Timestamp.max
        return data

    def run(self, *args, **kwargs):
        with self.get_session() as session:
            # select stocks still on
            max_dt = pd.to_datetime(session.query(
                sa.func.max(self.model.suspend_date).label('max_dt')
            ).one()[0]) - pd.Timedelta(days=5)

            if max_dt is pd.NaT:
                # data do not exist, download by stock code
                stock_list = pd.DataFrame(
                    session.query(
                        stock_org.AShareDescription.wind_code,
                        stock_org.AShareDescription.list_dt,
                    ).all()
                ).dropna()
                query_params = (dict(ts_code=code) for code in stock_list['wind_code'])
            else:
                # data exist, download by date
                query_params = ({key: f'{dt:%Y%m%d}'} for key, dt in product(
                    ('suspend_date', 'resume_date'),
                    (i for i in self.get_dates('D') if max_dt < i <= self.last_td_date)
                ))

        for q in query_params:
            self.bulk_insert(records=self.get_tushare_data(**q), model=self.model)

        self.clean_duplicates(self.model, [self.model.suspend_date, self.model.suspend_type, self.model.wind_code])


class AShareEODDerivativeIndicator(_CrawlerEOD):
    """
    Crawling stock end-of-date price from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=32
    """

    @property
    def model(self):
        return stock_org.AShareEODDerivativeIndicator

    def get_tushare_data_(self, **func_kwargs):
        self.get_logger().info(f'getting data from tushare: {func_kwargs}.')

        mapping = {
            'ts_code': 'wind_code',
            'trade_date': 'trade_dt',
            'tot_shr': 'share_tot',
            'float_a_shr': 'share_float',
            'free_shares': 'share_free',
            'turn': 'turnover',
            'free_turnover': 'turnover_free',
            'up_down_limit_status': 'suspend_status',
            'net_incr_cash_cash_equ_ttm': 'net_increase_cash_equ_ttm',
            'net_incr_cash_cash_equ_lyr': 'net_increase_cash_equ_lyr',
        }
        price = super().get_tushare_data(
            api_name='daily_basic',
            date_cols=['trade_date'], col_mapping=mapping,
            fields=[*mapping.keys(), 'pe', 'pb_new', 'pe_ttm', 'pcf_ocf', 'pcf_ocf_ttm', 'pcf_ncf', 'pcf_ncf_ttm',
                    'ps', 'ps_ttm', 'price_div_dps', 'close', 'adj_high_52w',
                    'adj_low_52w', 'net_assets', 'net_profit_parent_comp_ttm', 'net_profit_parent_comp_lyr',
                    'net_cash_flows_oper_act_ttm', 'net_cash_flows_oper_act_lyr', 'oper_rev_ttm', 'oper_rev_lyr'],
            **func_kwargs
        )
        return price


class AShareIndustry(TushareCrawlerJob):

    @property
    def enum_tb(self):
        return enum_code.EnumIndustryCode

    def run(self, *args, **kwargs):
        for code, data in (self.get_zz_industry()):
            self.upsert_data(
                data,
                model=stock_org.AShareSector,
                ukeys=stock_org.AShareSector.u_key.columns,
                msg=code
            )

    def get_zz_industry(self):
        with self.get_session() as session:
            industry_codes = session.query(
                self.enum_tb.code
            ).filter(
                self.enum_tb.level_num == 3,
                sa.func.substr(self.enum_tb.code, 1, 2) == SectorEnum.SEC_ZZ.value
            ).all()

        for (code,) in industry_codes:
            data = self.get_tushare_data(
                api_name='index_member_zz',
                date_cols=['entry_dt', 'remove_dt'],
                col_mapping={'ts_code': 'wind_code', 'index_code': 'sector_code'},
                index_code=code
            ).drop('is_new', axis=1, errors='ignore').fillna({'remove_dt': pd.Timestamp.max})
            yield code, data

    def get_sw_industry(self):
        # TODO: not exist in prod, still have problem in dev server.
        with self.get_session() as session:
            industry_codes = session.query(
                self.enum_tb.code,
                self.enum_tb.memo,
            ).filter(
                self.enum_tb.level_num == 4,
                sa.func.substr(self.enum_tb.code, 1, 2) == SectorEnum.SEC_SW.value
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


if __name__ == '__main__':
    from paramecium.database import create_all_table

    create_all_table()
    AShareDescription().run()
    AShareEODDerivativeIndicator().run()
    ASharePrice().run()
    AShareSuspend().run()
    AShareIndustry().run()
