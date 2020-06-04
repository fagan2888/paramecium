# -*- coding: utf-8 -*-
"""
@Time: 2020/6/4 9:06
@Author: Sue Zhu
"""
import pandas as pd
import sqlalchemy as sa

from paramecium.database import index_org
from paramecium.scheduler_.jobs._localizer import TushareCrawlerJob
from paramecium.utils.configuration import get_type_codes


class IndexDescFromTS(TushareCrawlerJob):
    """
    Crawling index description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=94
    """

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
        self.upsert_data(data, index_org.IndexDescription, index_org.IndexDescription.get_primary_key(), func_kwargs)

    def run(self, check_price_exist=1, *args, **kwargs):
        types = get_type_codes('index_org_description')

        types['publisher'].pop('WIND')
        for idx_type in types['index_code'].keys():
            self.get_and_insert_data(market='WIND', category=idx_type)

        for pub in types['publisher'].keys():
            self.get_and_insert_data(market=pub)

        if check_price_exist:
            self.get_price_code()

    def get_price_code(self):
        model = index_org.IndexDescription
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


class IndexDescWind():
    pass


class IndexPriceFromTS(TushareCrawlerJob):
    """
    Crawling index description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=94
    """

    def run(self, initial=0, *args, **kwargs):
        pass

    def get_eod_data(self, **kwargs):
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
            api_name='index_daily', date_cols=['trade_dt'],
            col_mapping=mapping, fields=list(mapping.keys()), **kwargs
        )
        return data


if __name__ == '__main__':
    from paramecium.database import create_all_table

    create_all_table()
    IndexDescFromTS().run()
    IndexPriceFromTS(env='tushare_prod')
