# -*- coding: utf-8 -*-
"""
@Time: 2020/6/4 9:06
@Author: Sue Zhu
"""
import numpy as np
import pandas as pd
import sqlalchemy as sa

from .._models import index
from ..comment import get_last_td
from ._base import *
from .._tool import get_type_codes


class IndexDescFromTS(TushareCrawlerJob):
    """
    Crawling index description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=94
    """

    def run(self, check_price_exist=1, env='prod', *args, **kwargs):
        super().run(env, *args, **kwargs)
        types = get_type_codes('index_org_description')

        types['publisher'].pop('WIND')
        for idx_type in types['index_code'].keys():
            self.get_and_insert_data(market='WIND', category=idx_type)

        for pub in types['publisher'].keys():
            self.get_and_insert_data(market=pub)

        if check_price_exist:
            self.get_price_code()

    def get_and_insert_data(self, **func_kwargs):
        data = self.get_tushare_data(
            api_name='index_basic',
            date_cols=['index_base_per', 'list_date', 'expire_date'],
            fields=['market', 'list_date', 'index_code', 'publisher', 'index_style',
                    'index_intro', 'weight_type', 'expire_date', 'income_processing_method', 'change_history'],
            **func_kwargs
        )
        data.loc[:, 'index_code'] = data['index_code'].dropna().map(lambda x: f'{x:.0f}')
        data = data.fillna({'index_code': '647000000', 'expire_date': pd.Timestamp.max})
        self.insert_data(data, index.Description, index.Description.get_primary_key(), func_kwargs)

    def get_price_code(self):
        model = index.Description
        with get_session() as ss:
            index_codes = ss.query(model.wind_code).filter(
                model.expire_date > sa.func.current_date(),
                model.index_code.in_([
                    "647000000", "647002000", "647002001", "647002002",  # "647002003", "647002004", # "行业指数",
                    "647003000", "647004000", "647004001", "647004002", "647005000", "647001000", ]),
                model.localized == 0,
            ).all()

            has_price = []
            no_price = []
            for (code,) in index_codes:
                try:
                    data = self.get_tushare_data(api_name='index_daily', ts_code=code)
                    if data.empty:
                        no_price.append(code)
                    else:
                        has_price.append(code)
                except Exception as e:
                    self.get_logger().error(repr(e))
                    break

            ss.query(model).filter(
                model.wind_code.in_(has_price)
            ).update(dict(localized=1, updated_at=sa.func.current_timestamp()), synchronize_session='fetch')
            ss.query(model).filter(
                model.wind_code.in_(no_price)
            ).update(dict(localized=-1, updated_at=sa.func.current_timestamp()), synchronize_session='fetch')


class IndexDescWind(BaseLocalizerJob):
    """
    Crawler Index data from wind api
    """
    codes = {
        '647006000': [
            '885000.WI', '885001.WI', '885002.WI', '885003.WI', '885004.WI', '885005.WI', '885006.WI', '885007.WI',
            '885008.WI', '885009.WI', '885012.WI', '885013.WI', '885054.WI', '885061.WI', '885062.WI', '885063.WI',
            '885064.WI', '885065.WI', '885066.WI', '885067.WI', '885068.WI', '885069.WI', '885070.WI', '930609.CSI',
            '930610.CSI', '930890.CSI', '930891.CSI', '930892.CSI', '930893.CSI', '930894.CSI', '930897.CSI',
            '930898.CSI', '930950.CSI', '930994.CSI', '930995.CSI', '931153.CSI', 'h11021.CSI', 'h11022.CSI',
            'h11023.CSI', 'h11024.CSI', 'h11025.CSI', 'h11026.CSI', 'h11027.CSI', 'h11028.CSI'],
        '647007000': [
            'CBA00301.CS', 'CBA02501.CS', 'CBA03801.CS', 'CBA04201.CS', 'h11001.CSI', 'h11015.CSI', ]
    }

    def run(self):
        desc_mp = {
            'SEC_NAME': 'short_name',
            'BASEVALUE': 'base_point',
            'BASEDATE': 'base_date',
            'METHODOLOGY': 'weights_rule',
            'REPO_BRIEFING': 'index_intro',
            'EXCH_ENG': 'market',
            'LAUNCHDATE': 'list_date',
            'DELIST_DATE': 'expire_date',
            'CRM_ISSUER': 'publisher'
        }
        with get_wind_api() as w:
            for type_code, idx_codes in self.codes.items():
                error, desc = w.wss(idx_codes, list(desc_mp.keys()), usedf=True)  # wss限8000单元格
                if error:
                    self.get_logger().error(error)
                else:
                    desc = desc.fillna(np.nan).rename(columns=desc_mp).assign(
                        wind_code=desc.index.map(lambda x: x.replace('H', 'h')),
                        index_code=type_code
                    )
                    for t_col in ('base_date', 'list_date', 'expire_date'):
                        desc.loc[:, t_col] = pd.to_datetime(
                            desc[t_col].where(lambda ser: ser > '1899-12-30', pd.Timestamp.max)
                        )
                    self.insert_data(records=desc, model=index.Description, ukeys=index.Description.get_primary_key())

                self.localized_price_from_wind(desc, w)

        self.clean_duplicates(
            index.EODPrice,
            [index.EODPrice.wind_code, index.EODPrice.trade_dt]
        )

    def localized_price_from_wind(self, desc, w_api):
        price_mp = {
            'OPEN': 'open_',
            'HIGH': 'high_',
            'LOW': 'low_',
            'CLOSE': 'close_',
            'VOLUME': 'volume_',
            'AMT': 'amount_',
            'CURR': 'currency'
        }
        with get_session() as ss:
            desc_loc = desc.set_index('wind_code').assign(
                max_price=pd.to_datetime(
                    pd.DataFrame(
                        ss.query(
                            index.EODPrice.wind_code,
                            sa.func.max(index.EODPrice.trade_dt).label('max_dt')
                        ).group_by(index.EODPrice.wind_code).filter(
                            index.EODPrice.wind_code.in_(desc['wind_code'])
                        ).all(),
                        columns=['wind_code', 'max_dt'],
                    ).set_index('wind_code').squeeze()
                )
            )
            max_dt = desc_loc.loc[:, ['base_date', 'max_price']].max(axis=1).loc[lambda ser: ser < get_last_td()]

        for code, start in max_dt.items():
            for dt in pd.date_range(start - pd.Timedelta(days=5), pd.Timestamp.now(), freq='1000D'):
                # wsd限8000单元格
                error, price = w_api.wsd(code, [*price_mp.keys()], dt, dt + pd.Timedelta(days=1000), "", usedf=True)
                if error:
                    self.get_logger().error(error)
                else:
                    price = price.rename(columns=price_mp).assign(
                        wind_code=code, trade_dt=pd.to_datetime(price.index)
                    ).dropna(subset=['close_']).fillna(np.nan)
                    self.insert_data(price, index.EODPrice, msg=f'{code} - {dt:%Y%m%d}')


class IndexPriceFromTS(TushareCrawlerJob):
    """
    Crawling index description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=95
    """

    def run(self, env='prod', *args, **kwargs):
        super().run(env, *args, **kwargs)
        with get_session() as ss:
            code_list = pd.read_sql(
                """
                select
                    d.wind_code, d.base_date, max(iop.trade_dt) as max_dt
                from index_org_description d
                    left join index_org_price iop on d.wind_code = iop.wind_code
                where localized=1
                group by d.wind_code
                """,
                ss.bind, parse_dates=['base_date', 'max_dt'],
                index_col=['wind_code']
            )
            code_list = code_list.max(axis=1).loc[lambda ser: ser < get_last_td()]

        for code, start in code_list.items():
            try:
                for dt in pd.date_range(start - pd.Timedelta(days=5), pd.Timestamp.now(), freq='3000D'):
                    self.localized_eod_data(
                        ts_code=code, start_date=dt.strftime('%Y%m%d'),
                        end_date=min((dt + pd.Timedelta(days=2999), get_last_td())).strftime('%Y%m%d')
                    )
            except Exception as e:
                self.get_logger().error(repr(e))
                break

        self.clean_duplicates(
            index.EODPrice,
            [index.EODPrice.wind_code, index.EODPrice.trade_dt]
        )

    def localized_eod_data(self, **kwargs):
        data = self.get_tushare_data(
            api_name='index_daily', date_cols=['trade_date'], **kwargs
        ).filter(index.EODPrice.__dict__.keys(), axis=1)
        self.insert_data(records=data, model=index.EODPrice)
