# -*- coding: utf-8 -*-
"""
@Time: 2020/6/4 9:06
@Author: Sue Zhu
"""
import numpy as np
import pandas as pd
import sqlalchemy as sa

from paramecium.scheduler_.jobs._localizer import TushareCrawlerJob, BaseLocalizerJob
from paramecium.configuration import get_type_codes
from paramecium.backtest import get_wind_api


class IndexDescFromTS(TushareCrawlerJob):
    """
    Crawling index description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=94
    """

    def run(self, check_price_exist=1, *args, **kwargs):
        types = get_type_codes('index_org_description')

        types['publisher'].pop('WIND')
        for idx_type in types['index_code'].keys():
            self.get_and_insert_data(market='WIND', category=idx_type)

        for pub in types['publisher'].keys():
            self.get_and_insert_data(market=pub)

        if check_price_exist:
            self.get_price_code()

    def get_and_insert_data(self, **func_kwargs):
        col_map = {
            'ts_code': 'wind_code',
            'name': 'short_name',
            'comp_name': 'full_name',
            'index_base_per': 'base_date',
            'index_base_pt': 'base_point',
            'index_weights_rule': 'weights_rule'
        }
        data = self.get_tushare_data(
            api_name='index_basic',
            date_cols=['index_base_per', 'list_date', 'expire_date'],
            fields=[*col_map.keys(), 'market', 'list_date', 'index_code', 'publisher', 'index_style',
                    'index_intro', 'weight_type', 'expire_date', 'income_processing_method', 'change_history'],
            col_mapping=col_map,
            **func_kwargs
        )
        data.loc[:, 'index_code'] = data['index_code'].dropna().map(lambda x: f'{x:.0f}')
        data = data.fillna({'index_code': '647000000', 'expire_date': pd.Timestamp.max})
        self.upsert_data(data, index_org.Description, index_org.Description.get_primary_key(), func_kwargs)

    def get_price_code(self):
        model = index_org.Description
        with self.get_session() as ss:
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
                    self.upsert_data(desc, index_org.Description, index_org.Description.get_primary_key())

        self.clean_duplicates(
            index_org.EODPrice,
            [index_org.EODPrice.wind_code, index_org.EODPrice.trade_dt]
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
        with self.get_session() as ss:
            max_dt = pd.DataFrame(
                ss.query(
                    index_org.EODPrice.wind_code,
                    sa.func.max(index_org.EODPrice.trade_dt).label('max_dt')
                ).group_by(index_org.EODPrice.wind_code).filter(
                    index_org.EODPrice.wind_code.in_(desc.index)
                ).all(),
                columns=['wind_code', 'max_dt'],
            ).set_index('wind_code').reindex(index=desc['wind_code']).squeeze()
            max_dt = pd.to_datetime(max_dt).fillna(desc['base_date']).loc[lambda ser: ser < self.last_td_date]

        for code, start in max_dt.items():
            for dt in pd.date_range(start - pd.Timedelta(days=5), pd.Timestamp.now(), freq='1000D'):
                # wsd限8000单元格
                error, price = w_api.wsd(code, price_mp.keys(), dt, dt + pd.Timedelta(days=1000), "", usedf=True)
                if error:
                    self.get_logger().error(error)
                else:
                    price = price.rename(columns=price_mp).assign(
                        wind_code=code, trade_dt=pd.to_datetime(price.index)
                    ).dropna(subset=['close_']).fillna(np.nan)
                    self.bulk_insert(price, index_org.EODPrice, msg=f'{code} - {dt:%Y%m%d}')


class IndexPriceFromTS(TushareCrawlerJob):
    """
    Crawling index description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=95
    """

    def run(self, initial=0, *args, **kwargs):
        with self.get_session() as ss:
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
            code_list = code_list.max(axis=1).loc[lambda ser: ser < self.last_td_date]

        for code, start in code_list.items():
            try:
                for dt in pd.date_range(start - pd.Timedelta(days=5), pd.Timestamp.now(), freq='3000D'):
                    self.localized_eod_data(
                        ts_code=code,
                        start_date=dt.strftime('%Y%m%d'),
                        end_date=min((dt + pd.Timedelta(days=2999), self.last_td_date)).strftime('%Y%m%d')
                    )
            except Exception as e:
                self.get_logger().error(repr(e))
                break

        self.clean_duplicates(
            index_org.EODPrice,
            [index_org.EODPrice.wind_code, index_org.EODPrice.trade_dt]
        )

    def localized_eod_data(self, **kwargs):
        mapping = {
            'ts_code': 'wind_code',
            'trade_date': 'trade_dt',
            'crncy_code': 'currency',
            'open': 'open_',
            'high': 'high_',
            'low': 'low_',
            'close': 'close_',
            'volume': 'volume_',
            'amount': 'amount_'
        }
        data = self.get_tushare_data(
            api_name='index_daily', date_cols=['trade_date'],
            col_mapping=mapping, fields=list(mapping.keys()), **kwargs
        )
        self.bulk_insert(data, index_org.EODPrice)


if __name__ == '__main__':
    from paramecium.database._postgres import create_all_table, index_org

    create_all_table()
    IndexDescFromTS().run(check_price_exist=0)
    IndexDescFromTS(env='tushare_dev').run()
    IndexDescFromTS(env='tushare_prod_old').run()
    IndexDescWind().run()
    IndexPriceFromTS(env='tushare_prod').run()
    IndexPriceFromTS(env='tushare_prod_old').run()
