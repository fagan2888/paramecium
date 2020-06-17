# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 20:58
@Author: Sue Zhu
"""
import json

import numpy as np
import pandas as pd
import sqlalchemy as sa

from ._base import *
from ..comment import get_dates, get_last_td
from ..pg_models import fund
from ... import const, utils


class FundDescription(CrawlerJob):
    """
    Crawler fund descriptions from tushare
    """

    @property
    def w_info(self):
        return get_wind_conf('crawler_mf_desc')['fields']

    def run(self, *args, **kwargs):
        model = fund.Description

        self.get_logger().debug('getting fund list from wind')
        w_set = pd.concat((
            self.query_wind(
                api_name='wset', tablename="sectorconstituent",
                options=f"date={pd.Timestamp.now():%Y-%m-%d};sectorid={sec_id};field=wind_code"
            ) for sec_id in ('1000027452000000', '1000008492000000')
        )).squeeze()
        w_set = w_set.loc[lambda ser: ~ser.str.split('.').str[0].duplicated(keep='first')]

        self.get_logger().debug('getting description from wind')
        w_desc = pd.concat((
            self.query_wind(
                api_name='wss', codes=funds, fields=[*self.w_info.keys()], col_mapping=self.w_info
            ) for funds in utils.chunk(w_set.values, 800)
        ), axis=0, sort=False)
        w_desc.loc[:, 'is_initial'] = (w_desc['is_initial'] == '是').astype(int)
        w_desc['wind_code'], w_desc.index = w_desc.index, w_desc.index.str.replace('!', '')

        self.get_logger().debug('deal with grad funds.')
        grad = w_desc.dropna(subset=['grad_type']).pivot('full_name', 'grad_type', 'wind_code')
        grad = grad.fillna({'分级基金优先级': grad['分级基金母基金']})
        # wind think grad A as initial, adjust to parent.
        w_desc.loc[lambda df: df['wind_code'].isin(grad['分级基金优先级']), 'is_initial'] = 0
        w_desc.loc[lambda df: df['wind_code'].isin(grad['分级基金母基金']), 'is_initial'] = 1
        # insert connections
        for chn, eng in (('分级基金优先级', 'A'), ('分级基金普通级', 'B')):
            self.insert_data(
                records=grad.loc[:, ['分级基金母基金', chn]].rename(columns={
                    '分级基金母基金': 'parent_code', '分级基金优先级': 'child_code', '分级基金普通级': 'child_code'
                }).assign(connect_type=eng).dropna(subset=['parent_code']),
                model=fund.Connections, ukeys=fund.Connections.uk.columns, msg=eng
            )

        self.get_logger().debug('deal with etf feeder funds.')
        self.insert_data(
            records=w_desc.loc[:, ['etf_code', 'wind_code']].dropna(subset=['etf_code']).rename(columns={
                'etf_code': 'parent_code', 'wind_code': 'child_code'
            }).assign(connect_type='F'),
            model=fund.Connections, ukeys=fund.Connections.uk.columns, msg='F'
        )

        self.get_logger().debug('getting description from tushare')
        ts_desc = pd.concat(
            (self.get_tushare_data(api_name='fund_basic', market=m) for m in list('OE')),
            axis=0, sort=False
        ).set_index('wind_code').filter(model.__dict__.keys(), axis=1)
        ts_desc.loc[
            lambda df: df['setup_date'].notnull() & df['maturity_date'].isnull(), 'maturity_date'] = pd.Timestamp.max

        self.get_logger().debug('merge wind and tushare data.')
        desc = pd.concat((w_desc.drop(['grad_type', 'etf_code'], axis=1, errors='ignore'), ts_desc), axis=1, sort=False)
        self.get_logger().info('saving description into database')
        self.insert_data(records=desc.dropna(subset=['wind_code']), model=model, ukeys=[model.wind_code])


class FundSales(CrawlerJob):
    meta_args = (
        {'type': 'int', 'description': 'number more than sale funds.'},  # n_records
    )

    def run(self, n_records=3000, *args, **kwargs):
        responds_raw = self.request_from_web(
            url=f"https://wpt.xcsc.com/lcsc/servlet/json?funcNo=24030123&page=1&numPerPage={n_records}"
        )
        responds_json = json.loads(responds_raw.text)['results'][0]
        if responds_json['totalPages'] > 1:
            self.get_logger().warn(
                f"the results not cover all funds for sale, please make "
                f"`n_records` more than {responds_json['totalRows']}.")
        responds_df = pd.DataFrame(responds_json['data']).reindex(columns=[
            'product_code', 'product_id', 'product_abbr', 'risk_level', 'per_buy_limit', 'product_status',
            'subscribe_start_time', 'subscribe_end_time', 'purchase_rates', 'purchase_rates_dis',
        ]).replace('', np.nan)
        for c in ('product_id', 'risk_level', 'product_status'):
            responds_df.loc[:, c] = pd.to_numeric(responds_df[c], errors='coerce').astype(int)
        for c in ('per_buy_limit', 'purchase_rates', 'purchase_rates_dis'):
            responds_df.loc[:, c] = pd.to_numeric(responds_df[c], errors='coerce').astype(float)
        for c in ('subscribe_start_time', 'subscribe_end_time'):
            responds_df.loc[:, c] = pd.to_datetime(responds_df[c].replace('0', np.nan), format='%Y%m%d')

        self.insert_data(responds_df, model=fund.WebSaleList, ukeys=[fund.WebSaleList.product_code])


class FundNav(CrawlerJob):
    """
    Crawler fund net asset values from tushare
    Since tushare api can only request 10,000 times per hour,
    use `try-except` to get most.
    """

    def run(self, *args, **kwargs):
        self.get_logger().info('query exist nav data to get query range')
        with get_session() as session:
            max_dts = pd.read_sql(
                """
                select t.wind_code, t.max_dt
                from (
                    select 
                        d.*,
                        case when p.max_dt is null then d.setup_date else p.max_dt end as max_dt
                    from mf_org_description d 
                    left join (select wind_code, max(trade_dt) as max_dt from mf_org_nav group by wind_code) p 
                    on d.wind_code=p.wind_code
                    where d.setup_date>'1990-01-01'
                    and status <> 'L'
                ) t             
                where 
                    t.maturity_date - t.max_dt > -5
                """,
                session.bind, parse_dates=['max_dt'], index_col=['wind_code']
            ).squeeze().loc[lambda ser: ser < get_last_td()]  # (ser.index.str.len() < 10) &
        max_dts -= pd.Timedelta(days=7)

        nav = pd.DataFrame()
        for i, (code, dt) in enumerate(max_dts.items(), start=1):
            try:
                self.get_logger().info(f'getting {code} nav from tushare')
                nav = pd.concat((
                    nav,
                    self.get_tushare_data(api_name='fund_nav', ts_code=code)
                ), axis=0).loc[lambda df: df['trade_dt'] >= dt]
            except Exception as e:
                self.get_logger().error(f'error happends when run {repr(e)}')
                break

            if nav.shape[0] > 10000:
                nav = self.insert_nav(nav, i / max_dts.shape[0])

        self.insert_nav(nav, 1)
        self.clean_duplicates(fund.Nav, [fund.Nav.wind_code, fund.Nav.trade_dt])

    def insert_nav(self, nav, pct):
        nav['adj_factor'] = nav['adj_nav'].div(nav['unit_nav']).round(6)
        self.insert_data(
            records=nav.drop(['accum_div', 'adj_nav'], axis=1, errors='ignore'),
            model=fund.Nav, msg=f'fund navs({pct * 100:.2f}%)'
        )
        return pd.DataFrame()


class FundManager(CrawlerJob):
    """
    Crawler fund net asset values from tushare
    """

    def run(self, *args, **kwargs):
        model = fund.ManagerHistory

        with get_session() as session:
            fund_list = pd.DataFrame(session.query(fund.Description.wind_code)).squeeze()

        for funds in utils.chunk(fund_list, 100):
            managers = self.get_tushare_data(
                api_name='fund_manager', ts_code=','.join(funds),
                fields=['wind_code', 'ann_date', 'manager_name', 'start_dt', 'end_dt'],
            ).fillna({
                'start_dt': pd.Timestamp.min,
                'end_dt': pd.Timestamp.max,
            })
            self.insert_data(managers, model, ukeys=model.uk.columns)


class FundSector(CrawlerJob):
    """
    Crawler fund sector data from wind api
    """

    def query_and_insert(self, type_, freq, codes):
        with get_session() as ss:
            max_dt, = ss.query(
                sa.func.max(fund.SectorSnapshot.trade_dt).label('max_dt')
            ).filter(fund.SectorSnapshot.type_ == type_).one()
        if max_dt is not None:
            max_dt = pd.to_datetime(max_dt)
        else:
            max_dt = pd.Timestamp('2009-12-30')

        for dt in (t for t in get_dates(freq) if max_dt < t <= get_last_td()):
            sector_list = pd.concat((self.query_wind(
                api_name='wset', tablename="sectorconstituent", usedf=True,
                options=f"date={dt:%Y-%m-%d};sectorid={sector};field=wind_code",
            ).assign(trade_dt=dt, sector_code=sector, type_=sector[:4]) for sector in codes))
            self.insert_data(sector_list, fund.SectorSnapshot, msg=f'{dt:%Y%m%d}')

    def run(self, *args, **kwargs):
        # '200101x'按底层资产分类
        self.query_and_insert(
            '2001', const.FreqEnum.M,
            (
                '2001010101000000', '2001010102000000', '2001010103000000', '2001010201000000',
                '2001010202000000', '2001010203000000', '2001010204000000', '2001010301000000',
                '2001010302000000', '2001010303000000', '2001010304000000', '2001010305000000',
                '2001010306000000', '2001010400000000'
            )
        )
        # '1000x'特殊分类
        self.query_and_insert(
            '1000', const.FreqEnum.Q,
            # 定期开放,委外,机构,可转债
            ("1000007793000000", "1000027426000000", "1000031885000000", "1000023509000000")
        )
