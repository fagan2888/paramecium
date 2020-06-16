# -*- coding: utf-8 -*-
"""
@Time: 2020/6/4 9:06
@Author: Sue Zhu
"""
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import Query

from ._base import CrawlerJob
from .._postgres import get_session
from .._third_party_api import get_wind_conf
from .._tool import get_type_codes
from ..comment import get_last_td
from ..pg_models import index
from ...utils import chunk


class IndexDescription(CrawlerJob):
    """
    Crawling index description
        1. from tushare, mainly a share index
        http://tushare.xcsc.com:7173/document/2?doc_id=94
        2. wind api
        bond and fund index not available from tushare, conf key `crawler_index_desc`
    """

    def run(self, *args, **kwargs):
        types = get_type_codes('index_org_description')

        self.get_logger().info('Localized from tushare api')
        types['publisher'].pop('WIND')
        for idx_type in types['index_code'].keys():
            self.get_and_insert_ts(market='WIND', category=idx_type)

        for pub in types['publisher'].keys():
            self.get_and_insert_ts(market=pub)

        self.get_logger().info('Localized from wind api')
        w_conf = get_wind_conf('crawler_index_desc')
        chunk_size = 8000 // len(w_conf['fields'].keys())  # wss限8000单元格
        codes = {c: tp for tp, c_list in get_wind_conf('crawler_index') for c in c_list}
        for codes in chunk(codes.keys(), chunk_size):
            desc = self.query_wind(
                api_name='wss', codes=codes, fields=list(w_conf['fields'].keys()),
                col_mapping=w_conf['fields'], dt_cols=w_conf['dt_cols']
            ).assign(localized=-1)
            desc['index_code'] = desc['wind_code'].map(codes)
            self.insert_data(records=desc, model=index.Description, ukeys=index.Description.get_primary_key())

    def get_and_insert_ts(self, **func_kwargs):
        data = self.get_tushare_data(
            api_name='index_basic',
            **func_kwargs
        ).filter(index.Description.__dict__.keys(), axis=1)
        data.loc[:, 'index_code'] = data['index_code'].dropna().map(lambda x: f'{x:.0f}')
        data = data.fillna({'index_code': '647000000', 'expire_date': pd.Timestamp.max})
        self.insert_data(data, index.Description, index.Description.get_primary_key(), func_kwargs)


class IndexPrice(CrawlerJob):
    """
    Crawling index description from tushare
    http://tushare.xcsc.com:7173/document/2?doc_id=95
    """
    ts_limit = 2999

    def run(self, check_new=1, *args, **kwargs):
        self.get_logger().debug('localize wind api')
        with get_session() as ss:
            price_group = Query([
                index.EODPrice.wind_code,
                sa.func.max(index.EODPrice.trade_dt).label('max_dt')
            ]).group_by(
                index.EODPrice.wind_code
            ).subquery('g')
            desc = pd.DataFrame(
                ss.query(
                    index.Description.wind_code,
                    index.Description.base_date,
                    price_group.c.max_dt,
                    index.Description.localized
                ).join(
                    price_group,
                    index.Description.wind_code == price_group.c.wind_code,
                    isouter=True  # left join
                ).all()
            ).set_index('wind_code')
            desc['max_dt'] = desc.loc[:, ['base_date', 'max_dt']].apply(pd.to_datetime).max(axis=1)
            desc = desc.loc[lambda df: df['max_dt'] < get_last_td()]

        self.get_logger().debug('localize wind api')
        codes = [c for _, c_list in get_wind_conf('crawler_index').items() for c in c_list]
        w_conf = {k.upper(): v for k, v in get_wind_conf('crawler_index_desc')['fields'].items()}
        for code, start in desc.filter(codes, axis=0)['max_dt'].items():
            for dt in pd.date_range(start - pd.Timedelta(days=5), pd.Timestamp.now(), freq='1000D'):  # wsd限8000单元格
                price = self.query_wind(
                    api_name='wsd', codes=code, fields=[*w_conf.keys()],
                    beginTim=dt, endTime=max((dt + pd.Timedelta(days=1000), get_last_td())),
                    options="", col_mapping=w_conf
                ).dropna(subset=['close_'])
                self.insert_data(price.assign(
                    wind_code=code, trade_dt=pd.to_datetime(price.index)
                ), index.EODPrice, msg=f'{code} - {dt:%Y%m%d}')

        self.get_logger().debug('localize listed ts code.')
        for code, start in desc.loc[desc['localized'].eq(1), 'max_dt'].items():
            try:
                for dt in pd.date_range(start - pd.Timedelta(days=5), pd.Timestamp.now(), freq=f'{self.ts_limit}D'):
                    self.localized_ts(ts_code=code, start_date=dt.strftime('%Y%m%d'))
            except Exception as e:
                self.get_logger().error(repr(e))
                break

        if check_new:
            self.get_logger().debug('check new code.')
            with get_session() as ss:
                index_codes = ss.query(
                    index.Description.wind_code,
                    index.Description.base_date
                ).filter(
                    index.Description.expire_date > sa.func.current_date(),
                    index.Description.index_code.in_([
                        "647000000", "647002000", "647002001", "647002002", "647002003", "647002004",  # "行业指数",
                        "647003000", "647004000", "647004001", "647004002", "647005000", "647001000", ]),
                    index.Description.localized == 0,
                ).all()

                has_price = []
                no_price = []
                for (code, base_dt) in index_codes:
                    try:
                        for dt in pd.date_range(base_dt, pd.Timestamp.now(), freq=f'{self.ts_limit}D'):
                            ts_price = self.localized_ts(ts_code=code, start_date=dt.strftime('%Y%m%d'))
                            if ts_price.empty:
                                no_price.append(code)
                                break
                        else:
                            has_price.append(code)
                    except Exception as e:
                        self.get_logger().error(repr(e))
                        break

                ss.query(index.Description).filter(
                    index.Description.wind_code.in_(has_price)
                ).update(dict(localized=1, updated_at=sa.func.current_timestamp()), synchronize_session='fetch')
                ss.query(index.Description).filter(
                    index.Description.wind_code.in_(no_price)
                ).update(dict(localized=-1, updated_at=sa.func.current_timestamp()), synchronize_session='fetch')

        self.clean_duplicates(
            index.EODPrice,
            [index.EODPrice.wind_code, index.EODPrice.trade_dt]
        )

    def localized_ts(self, ts_code, start_date):
        data = self.get_tushare_data(
            api_name='index_daily',
            ts_code=ts_code, start_date=start_date.strftime('%Y%m%d'),
            end_date=min((start_date + pd.Timedelta(days=self.ts_limit), get_last_td())).strftime('%Y%m%d')
        ).filter(index.EODPrice.__dict__.keys(), axis=1)
        self.insert_data(records=data, model=index.EODPrice)
        return data
