# -*- coding: utf-8 -*-
"""
@Time: 2020/5/12 20:58
@Author: Sue Zhu
"""
import json

import numpy as np
import pandas as pd
import sqlalchemy as sa
from pandas.tseries.offsets import QuarterEnd

from ._base import *
from ..comment import get_last_td
from ..pg_models import fund, others
from ... import utils


class FundDescription(CrawlerJob):
    """
    Crawler fund descriptions from tushare
    """
    TS_ENV = 'dev'

    @property
    def w_info(self):
        return get_wind_conf('crawler_mf_desc')['fields']

    def run(self, *args, **kwargs):
        model = fund.Description

        self.get_logger().debug('getting description from tushare')
        fields = [c.key for c in model.__table__.c if c.key not in ('wind_code', 'updated_at')]
        desc = pd.concat(
            (self.get_tushare_data(
                api_name='fund_description', fields=['wind_code', *f]
            ).set_index('wind_code') for f in utils.chunk(fields, 5)),
            axis=1, sort=False
        )
        desc = pd.concat((desc, self.temp_localized(desc.index)), axis=0)
        desc.loc[desc['setup_date'].notnull() & desc['maturity_date'].isnull(), 'maturity_date'] = pd.Timestamp.max
        desc['wind_code'] = desc.index
        self.insert_data(records=desc, model=model, ukeys=[model.wind_code])

        with get_session() as ss:
            n_con, = ss.query(sa.func.count(fund.Connections.parent_code)).first()
        if n_con < 1:
            # only update grad fund at first localization.
            self.get_logger().debug('deal with grad funds.')
            grad = desc.assign(grad=self.wind_wss('FUND_SMFTYPE', desc.index)).dropna(subset=['grad'])
            grad_pvt = grad.pivot('full_name', 'grad', 'wind_code').fillna({'分级基金优先级': grad['分级基金母基金']})
            for chn, eng in (('分级基金优先级', 'A'), ('分级基金普通级', 'B')):
                self.insert_data(
                    records=grad_pvt.loc[:, ['分级基金母基金', chn]].rename(columns={
                        '分级基金母基金': 'parent_code', '分级基金优先级': 'child_code', '分级基金普通级': 'child_code'
                    }).assign(connect_type=eng).dropna(subset=['parent_code']),
                    model=fund.Connections, ukeys=fund.Connections.uk.columns, msg=eng
                )

        self.get_logger().debug('deal with etf feeder funds.')
        etf = self.wind_wss('FUND_ETFWINDCODE', desc.index)
        self.insert_data(
            records=[dict(parent_code=k, child_code=v, connect_type='F') for k, v in etf.items()],
            model=fund.Connections, ukeys=fund.Connections.uk.columns, msg='F'
        )

        with get_session() as ss:
            ss.query(model).filter(
                model.wind_code.in_(fund.Connections.child_code),
                model.wind_code.in_(fund.Connections.connect_type == 'F')
            ).update(dict(is_initial=0), synchronize_session='fetch')
            ss.query(model).filter(
                model.wind_code.in_(fund.Connections.parent_code)
            ).update(dict(is_initial=1), synchronize_session='fetch')

    def wind_wss(self, field, funds):
        return pd.concat(
            (self.query_wind('wss', codes=f, fields=[field]) for f in utils.chunk(funds, 7500)),
            axis=0, sort=False
        ).squeeze().dropna()

    def temp_localized(self, exists_codes):
        self.get_logger().debug('temporary localized from wind')
        exists_codes_symbol = {c.split('.')[0] for c in exists_codes}

        w_set = pd.concat((
            self.query_wind(
                api_name='wset', tablename="sectorconstituent",
                options=f"date={pd.Timestamp.now():%Y-%m-%d};sectorid={sec_id};field=wind_code"
            ) for sec_id in ('1000027452000000', '1000008492000000')
        )).squeeze()
        w_set = w_set.loc[lambda ser: ~ser.str.split('.').str[0].duplicated(keep='first')].to_list()

        self.get_logger().debug('getting description from tushare')
        ts_desc = pd.concat(
            (self.get_tushare_data(api_name='fund_basic', market=m) for m in 'OE'), axis=0
        ).set_index('wind_code')

        self.get_logger().debug('getting description from wind')
        w_desc = pd.concat((
            self.query_wind(
                api_name='wss', codes=funds, fields=self.w_info.keys(), col_mapping=self.w_info
            ) for funds in utils.chunk((c for c in w_set if c.split('.')[0] not in exists_codes_symbol), 800)
        ), axis=0, sort=False)
        w_desc.loc[:, 'is_initial'] = (w_desc['is_initial'] == '是').astype(int)
        w_desc['wind_code'], w_desc.index = w_desc.index, w_desc.index.str.replace('!', '')

        self.get_logger().debug('merge wind and tushare data.')
        desc = pd.concat((w_desc, ts_desc), axis=1, sort=False).set_index('wind_code', drop=False)
        return desc.dropna(subset=['wind_code'])


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
        max_dts = self.query_range()

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

    def query_range(self):
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
        return max_dts - pd.Timedelta(days=7)

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
        hist = fund.ManagerHistory
        info = fund.ManagerInfo

        with get_session() as session:
            fund_list = pd.DataFrame(session.query(fund.Description.wind_code)).squeeze()

        for code in fund_list:
            managers = self.get_tushare_data(
                api_name='fund_manager_1', ts_code=code
            ).fillna({'remove_dt': pd.Timestamp.max})
            self.insert_data(managers.rename(columns={'manager_id': 'status_code'}), hist, ukeys=hist.uk_.columns)
            self.insert_data(managers, info, ukeys=info.get_primary_key())


class FundSector(CrawlerJob):
    """
    Crawler fund sector data from tushare
    """
    TS_ENV = 'dev'

    def run(self, *args, **kwargs):
        m_code = others.EnumIndustryCode
        model = fund.Sector
        with get_session() as ss:
            sector_codes = ss.query(m_code.industry_code).filter(
                m_code.level_num > 3,
                sa.func.substr(m_code.industry_code, 1, 4) == '2001'
            ).all()
            max_dt = {k: v for k, v in ss.query(
                model.sector_code,
                sa.func.max(model.entry_dt)
            ).group_by(model.sector_code).all()}

        for (code,) in sector_codes:
            t = max_dt.get(pd.Timestamp(code), pd.Timestamp.min)
            ts_data = self.get_tushare_data(api_name='fund_sector', sector=code).fillna({'remove_dt': pd.Timestamp.max})
            if ts_data.shape[0] > 9999:
                self.get_logger().error('sector data may not localized entirely.')
            ts_data = ts_data.loc[(ts_data['entry_dt'] >= t) | (ts_data['remove_dt'] >= t)]
            self.insert_data(ts_data, model, ukeys=model.uk_.columns)


class FundPortfolioAsset(CrawlerJob):
    """
    基金资产配置数据
    """
    TS_ENV = 'dev'

    def run(self, *args, **kwargs):
        fund_list = self.get_max_dt().loc[lambda df: df['max_dt'] < '2018-12-31']
        for _, row in fund_list.iterrows():
            ts_data = self.get_tushare_data('asset_portfolio', ts_code=row['wind_code'])
            ts_data = ts_data.loc[lambda df: df['end_date'] >= row['max_dt'] - QuarterEnd(n=1)]
            self.insert_data(ts_data, fund.PortfolioAsset)
            self.insert_data(ts_data.loc[lambda df: df['bond_value'].gt(0)], fund.PortfolioAssetBond)

        # 临时的修补坑数据
        last_q = pd.Timestamp.now() - QuarterEnd(n=1)
        fund_list = self.get_max_dt().loc[lambda df: df['is_initial'].eq(1) & (df['max_dt'] < last_q)]
        mapping = get_wind_conf('crawler_mf_prf')
        for _, row in fund_list.iterrows():
            w_data = self.query_wind(
                api_name='wsd', codes=row['wind_code'], fields=mapping['fields'].keys(), col_mapping=mapping['fields'],
                beginTime=row['max_dt'] + QuarterEnd(n=1), endTime=last_q,
                options=mapping['options']
            ).assign(wind_code=row['wind_code'])
            if w_data.shape[0] < 2:
                w_data['end_date'] = row['max_dt'] + QuarterEnd(n=1)
            else:
                w_data['end_date'] = w_data.index
            self.insert_data(w_data, fund.PortfolioAsset, msg=row['wind_code'])

        self.clean_duplicates(fund.PortfolioAsset, [fund.PortfolioAsset.wind_code, fund.PortfolioAsset.end_date])
        self.clean_duplicates(
            fund.PortfolioAssetBond, [fund.PortfolioAssetBond.wind_code, fund.PortfolioAssetBond.end_date])

    @staticmethod
    def get_max_dt():
        with get_session() as ss:
            g = ss.query(
                fund.PortfolioAsset.wind_code,
                sa.func.max(fund.PortfolioAsset.end_date).label('max_dt')
            ).group_by(fund.PortfolioAsset.wind_code).subquery('g')
            fund_list = pd.DataFrame(
                ss.query(
                    fund.Description.wind_code,
                    fund.Description.setup_date,
                    fund.Description.maturity_date,
                    fund.Description.is_initial,
                    g.c.max_dt,
                ).join(
                    g, fund.Description.wind_code == g.c.wind_code, isouter=True  # left join
                ).order_by(fund.Description.is_initial.desc())
            )
        for c in ('setup_date', 'maturity_date', 'max_dt'):
            fund_list.loc[:, c] = pd.to_datetime(fund_list[c])
        fund_list = fund_list.fillna({'max_dt': fund_list['setup_date']})
        fund_list = fund_list.loc[
            lambda df: df['max_dt'] + QuarterEnd(n=0) < df['maturity_date'].clip(upper=get_last_td()) - QuarterEnd(n=1)]
        return fund_list
