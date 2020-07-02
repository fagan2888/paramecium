# -*- coding: utf-8 -*-
"""
@Time: 2020/6/23 9:39
@Author: Sue Zhu
"""
import pandas as pd
import sqlalchemy as sa
from pandas.tseries.offsets import QuarterEnd

from ._base import *
from .. import get_dates, get_last_td
from ..pg_models import fund
from ... import utils, const


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
                api_name='wss', codes=funds, fields=self.w_info.keys(), col_mapping=self.w_info
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


class FundSector(CrawlerJob):
    """
    Crawler fund sector data from wind api
    """

    def query_and_insert(self, freq, codes):
        for code in codes:
            with get_session() as ss:
                max_dt, = ss.query(
                    sa.func.max(fund.SectorSnapshot.trade_dt).label('max_dt')
                ).filter(fund.SectorSnapshot.sector_code == code).one()
            if max_dt is not None:
                max_dt = pd.to_datetime(max_dt)
            else:
                max_dt = pd.Timestamp('2009-12-30')

            for dt in (t for t in get_dates(freq) if max_dt < t <= get_last_td()):
                sector_list = self.query_wind(
                    api_name='wset', tablename="sectorconstituent",
                    options=f"date={dt:%Y-%m-%d};sectorid={code};field=wind_code",
                ).assign(trade_dt=dt, sector_code=code, type_=code[:4])
                if sector_list.empty:
                    continue
                sector_list.loc[:, 'wind_code'] = sector_list['wind_code'].map(self.code_mapping)
                self.insert_data(sector_list, fund.SectorSnapshot, msg=f'{dt:%Y%m%d} - {code}')

    def run(self, *args, **kwargs):
        with get_session() as ss:
            self.code_mapping = pd.DataFrame(
                ss.query(fund.Description.wind_code).all()
            ).squeeze()
            self.code_mapping.index = self.code_mapping.map(lambda x: x.split('.')[0] + '.OF').values
        # '200101x'按底层资产分类
        self.query_and_insert(
            const.FreqEnum.M,
            (
                '2001010101000000', '2001010102000000', '2001010103000000', '2001010201000000',
                '2001010202000000', '2001010203000000', '1000011486000000',  # '2001010204000000',
                '2001010301000000', '2001010302000000', '2001010303000000', '2001010304000000',
                '1000010419000000', '1000011421000000',  # '2001010305000000', '2001010306000000',
                '2001010400000000',
            )
        )
        # '1000x'特殊分类
        self.query_and_insert(
            const.FreqEnum.Q,
            # 定期开放,委外,机构,可转债,浮动净值,理财基金
            ("1000007793000000", "1000027426000000", "1000031885000000", "1000023509000000",
             "1000033974000000", "1000031776000000")
        )


class FundPortfolioAsset(CrawlerJob):
    """
    基金资产配置数据
    [2020/6/18] Only 规模数据
    w.wss("000001.OF,166005.OF,004232.OF", "prt_totalasset,prt_fundnetasset_total","unit=1;rptDate=20191231")
    """

    def run(self, *args, **kwargs):
        mapping = get_wind_conf('crawler_mf_prf')
        with get_session() as ss:
            max_dt, = ss.query(sa.func.max(fund.PortfolioAsset.end_date)).one()
            if max_dt is not None:
                max_dt = min((pd.to_datetime(max_dt), pd.Timestamp.now() - QuarterEnd(n=1)))
            else:
                max_dt = pd.Timestamp('2009-12-30')

            for quarter_end in pd.date_range(max_dt, pd.Timestamp.now(), freq='Q'):
                fund_list = [f for f, *_ in ss.query(
                    fund.Description.wind_code
                ).filter(
                    fund.Description.setup_date < quarter_end,
                    fund.Description.maturity_date >= quarter_end,
                    fund.Description.is_initial == 1,
                ).all()]
                for i, funds in enumerate(utils.chunk(fund_list, 1499), start=1):
                    data = self.query_wind(
                        api_name='wss', codes=funds, fields=mapping['fields'].keys(),
                        col_mapping=mapping['fields']
                    )
                    self.insert_data(
                        data.assign(end_date=quarter_end, wind_code=data.index),
                        fund.PortfolioAsset,
                        msg=f'{quarter_end} - {min(i * 1499 / 8000, 1) * 100:.2f}%'
                    )
        self.clean_duplicates(fund.PortfolioAsset, [fund.PortfolioAsset.wind_code, fund.PortfolioAsset.end_date])
