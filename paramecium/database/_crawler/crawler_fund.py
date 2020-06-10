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
from .._models import fund
from ..comment import get_dates, get_last_td
from ... import const, utils


class FundDescription(TushareCrawlerJob):
    """
    Crawler fund descriptions from tushare
    """
    w_info = {
        'CURR': 'currency',
        'FUND_PCHREDM_PCHMINAMT': 'min_purchase_amt',
        'EXCH_ENG': 'exchange',
        'FUND_INITIAL': 'is_initial',
        'FUND_INVESTSCOPE': 'invest_scope',
        'FUND_INVESTOBJECT': 'invest_object',
        'FUND_SALEFEERATIO': 'fee_ratio_sale',
        'FUND_FULLNAME': 'full_name',
        'fund_smftype'.upper(): 'grad_type',
        'fund_etfwindcode'.upper(): 'etf_code'
    }

    @staticmethod
    def wind2ts(codes):
        return codes.str.replace('!', '')

    def get_wind_info(self, funds, w):
        err, desc = w.wss(funds, [*self.w_info.keys()], usedf=True)
        if err:
            self.get_logger().error(f'Wind API has error {err}')
        else:
            return desc.rename(columns=self.w_info)

    def run(self, env='prod', *args, **kwargs):
        super().run(env, *args, **kwargs)
        model = fund.Description

        self.get_logger().info('Getting description from wind')
        with get_wind_api() as w:
            err, w_fund_list = w.wset(
                "sectorconstituent",
                f"date={pd.Timestamp.now():%Y-%m-%d};sectorid={const.WindSector.MF.value};field=wind_code",
                usedf=True
            )
            w_fund_list = w_fund_list.squeeze()
            w_fund_list.index = w_fund_list.str.replace('!', '')
            wind_desc = pd.concat((
                self.get_wind_info(funds, w) for funds in utils.chunk(w_fund_list.values, 800)
            )).rename(lambda x: x.replace('!', '')).assign(wind_code=w_fund_list)

            wind_desc.loc[:, 'is_initial'] = (wind_desc['is_initial'] == '是').astype(int)

            self.get_logger().info('Deal with grad funds.')
            grad = wind_desc.dropna(subset=['grad_type']).pivot('full_name', 'grad_type', 'wind_code')
            grad = grad.fillna({'分级基金优先级': grad['分级基金母基金']})
            # wind think grad A as initial, adjust to parent.
            wind_desc.loc[wind_desc['wind_code'].isin(grad['分级基金优先级']), 'is_initial'] = False
            wind_desc.loc[wind_desc['wind_code'].isin(grad['分级基金母基金']), 'is_initial'] = True
            # insert connections
            for chn, eng in (('分级基金优先级', 'A'), ('分级基金普通级', 'B')):
                self.insert_data(
                    records=grad.loc[:, ['分级基金母基金', chn]].rename(columns={
                        '分级基金母基金': 'parent_code', '分级基金优先级': 'child_code', '分级基金普通级': 'child_code'
                    }).assign(connect_type=eng),
                    model=fund.Connections, ukeys=fund.Connections.uk.columns, msg=eng
                )

            self.get_logger().info('Deal with etf feeder funds.')
            self.insert_data(
                records=wind_desc.dropna(subset=['etf_code']).loc[:, ['etf_code', 'wind_code']].rename(columns={
                    'etf_code': 'parent_code', 'wind_code': 'child_code'
                }).assign(connect_type='F'),
                model=fund.Connections, ukeys=fund.Connections.uk.columns, msg='F'
            )

        self.get_logger().info('Getting description from tushare')
        ts_desc = pd.concat(
            (self.get_tushare_data(
                api_name='fund_basic', market=m,
                date_cols=['found_date', 'due_date', 'issue_date', 'list_date', 'delist_date',
                           'purc_startdate', 'redm_startdate']
            ) for m in list('OE')),
            axis=0, sort=False
        ).set_index('wind_code').filter(model.__dict__.keys(), axis=1)

        ts_desc.loc[
            lambda df: df['setup_date'].notnull() & df['maturity_date'].isnull(), 'maturity_date'] = pd.Timestamp.max

        desc = pd.concat((
            wind_desc.drop(['grad_type', 'etf_code'], axis=1, errors='ignore'), ts_desc
        ), axis=1, sort=False).dropna(subset=['wind_code'])
        self.get_logger().info('Saving description into database')
        self.insert_data(records=desc, model=model, ukeys=[model.wind_code])



class FundSales(WebCrawlerJob):
    meta_args = (
        # n_records
        {'type': 'int', 'description': 'number more than sale funds.'},
    )

    def run(self, n_records=3000, *args, **kwargs):
        responds_raw = self.request(
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


class FundNav(TushareCrawlerJob):
    """
    Crawler fund net asset values from tushare
    Since tushare api can only request 10,000 times per hour,
    use `try-except` to get most.
    """

    def run(self, env='prod', *args, **kwargs):
        super().run(env, *args, **kwargs)
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
                ) t             
                where 
                    t.max_dt < t.maturity_date
                    and t.setup_date > date('1900-01-01')
                """,
                session.bind, parse_dates=['max_dt'], index_col=['wind_code']
            ).squeeze().loc[lambda ser: ser.index.str.len() < 10]
        max_dts -= pd.Timedelta(days=7)

        nav = pd.DataFrame()
        for i, (code, dt) in enumerate(max_dts.items(), start=1):
            try:
                self.get_logger().info(f'getting {code} nav from tushare')
                nav = pd.concat((
                    nav,
                    self.get_tushare_data(api_name='fund_nav', ts_code=code, date_cols=['end_date', 'ann_date'])
                ), axis=0).loc[lambda df: df['trade_dt'] >= dt]
            except Exception as e:
                self.get_logger().error(f'error happends when run {repr(e)}')
                break

            if nav.shape[0] > 10000:
                nav = self.insert_nav(nav, i / max_dts.shape[0])

        self.insert_nav(nav, 1)
        self.clean_duplicates(
            fund.Nav,
            [fund.Nav.wind_code, fund.Nav.trade_dt]
        )

    def insert_nav(self, nav, pct):
        nav['adj_factor'] = nav['adj_nav'].div(nav['unit_nav']).round(6)
        self.insert_data(
            records=nav.drop(['accum_div', 'adj_nav'], axis=1, errors='ignore'),
            model=fund.Nav, msg=f'fund navs({pct * 100:.2f}%)'
        )
        return pd.DataFrame()


class FundManager(TushareCrawlerJob):
    """
    Crawler fund net asset values from tushare
    """

    def run(self, env='prod', *args, **kwargs):
        super().run(env, *args, **kwargs)
        model = fund.ManagerHistory

        with get_session() as session:
            fund_list = pd.DataFrame(session.query(fund.Description.wind_code)).squeeze()

        for funds in utils.chunk(fund_list, 100):
            managers = self.get_tushare_data(
                api_name='fund_manager', ts_code=','.join(funds),
                date_cols=['ann_date', 'begin_date', 'end_date'], fields=['ann_date'],
            ).fillna({
                'start_dt': pd.Timestamp.min,
                'end_dt': pd.Timestamp.max,
            })
            self.insert_data(managers, model, ukeys=[model.wind_code, model.start_dt, model.manager_name])


class FundSector(BaseLocalizerJob):
    """
    Crawler fund sector data
    """

    def query_and_insert(self, api, type_, freq, codes):
        for month_end in (t for t in get_dates(freq) if self.get_max_dt(type_) < t <= get_last_td()):
            try:
                sector_list = pd.concat((self.get_sector_from_wind(month_end, sector, api) for sector in codes))
            except WindDataError:
                self.get_logger().error(repr(WindDataError))
                break
            else:
                self.insert_data(sector_list, fund.SectorSnapshot, msg=f'{month_end:%Y%m%d}')

    def run(self, *args, **kwargs):
        with get_wind_api() as w:
            # '200101x'按底层资产分类
            self.query_and_insert(
                w, '2001', const.FreqEnum.M,
                (
                    '2001010101000000', '2001010102000000', '2001010103000000', '2001010201000000',
                    '2001010202000000', '2001010203000000', '2001010204000000', '2001010301000000',
                    '2001010302000000', '2001010303000000', '2001010304000000', '2001010305000000',
                    '2001010306000000', '2001010400000000'
                )
            )
            # '1000x'特殊分类
            self.query_and_insert(
                w, '1000', const.FreqEnum.Q,
                # 定期开放,委外,机构,可转债
                ("1000007793000000", "1000027426000000", "1000031885000000", "1000023509000000")
            )

    @staticmethod
    def get_sector_from_wind(trade_dt, sector, api):
        err, sector_list = api.wset(
            tablename="sectorconstituent", usedf=True,
            options=f"date={trade_dt:%Y-%m-%d};sectorid={sector};field=wind_code",
        )
        if err == 0:
            return sector_list.assign(trade_dt=trade_dt, sector_code=sector, type_=sector[:4])
        else:
            raise WindDataError(f'Wind Data Api Error with {err}')

    @staticmethod
    def get_max_dt(code):
        with get_session() as ss:
            max_dt, = ss.query(
                sa.func.max(fund.SectorSnapshot.trade_dt).label('max_dt')
            ).filter(
                fund.SectorSnapshot.type_ == code
            ).one()
        if max_dt is not None:
            max_dt = pd.to_datetime(max_dt)
        else:
            max_dt = pd.Timestamp('2009-12-30')
        return max_dt
