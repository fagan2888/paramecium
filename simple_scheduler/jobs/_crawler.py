# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 23:32
@Author: Sue Zhu
"""
import logging
import random
from uuid import uuid4

import numpy as np
import pandas as pd
import sqlalchemy as sa
from ndscheduler.corescheduler import job
from requests import request
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker, scoped_session

from paramecium.tools.db_source import get_tushare_api, get_sql_engine


class _BaseCrawlerJob(job.JobBase):
    meta_args = None  # tuple of dict with type and description, both string.
    meta_args_example = ''  # string, json like
    _session_cls = None

    def __init__(self, job_id=None, execution_id=None):
        if job_id is None:
            job_id = uuid4()
        if execution_id is None:
            execution_id = uuid4()
        super().__init__(job_id, execution_id)

    @classmethod
    def get_model_name(cls):
        return f'{cls.__module__:s}.{cls.__name__:s}'

    @classmethod
    def meta_info(cls):
        """ 参数列表 """
        return {
            'job_class_string': cls.get_model_name(),
            'notes': cls.__doc__,
            'arguments': list(cls.meta_args) if cls.meta_args is not None else [],
            'example_arguments': cls.meta_args_example
        }

    @property
    def logger(self):
        return logging.getLogger(self.get_model_name())

    @property
    def sa_session(self):
        if _BaseCrawlerJob._session_cls is None:
            session_factory = sessionmaker(bind=get_sql_engine(env='postgres'))
            _BaseCrawlerJob._session_cls = scoped_session(session_factory)
        return _BaseCrawlerJob._session_cls()

    def upsert_data(self, records, model, ukeys=None, msg=''):
        self.logger.info(f'start to upsert data {msg}...')
        result_ids = []
        session = self.sa_session
        if isinstance(records, pd.DataFrame):
            records = (record.dropna() for _, record in records.iterrows())
        try:
            for record in records:
                insert_exe = insert(model).values(**record, updated_at=sa.text('current_timestamp'))
                if ukeys:
                    insert_exe = insert_exe.on_conflict_do_update(
                        index_elements=ukeys,
                        set_={k: sa.text(f'EXCLUDED.{k}') for k in record.keys() if k not in (c.key for c in ukeys)}
                    )
                exe_result = session.execute(insert_exe)
                result_ids.extend(exe_result.inserted_primary_key)
        except Exception as e:
            self.logger.warning(f'fail to insert data with {repr(e)}.')
            session.rollback()
        else:
            session.commit()
        return result_ids

    def truncate_table(self, model):
        name = model.__tablename__
        session = self.sa_session
        try:
            self.logger.info(f'truncate table {name:s}.')
            session.execute(f'truncate table {name:s};')
        except Exception as e:
            self.logger.warning(f'fail to truncate table before insert with {repr(e)}.')
            session.rollback()
        else:
            session.commit()

    @property
    def last_td_date(self):
        cur_date = pd.Timestamp.now()
        if cur_date.hour <= 22:
            cur_date -= pd.Timedelta(days=1)
        date_from_db = pd.read_sql(
            f"select max(trade_dt) from trade_calendar where is_d=1 and trade_dt<='{cur_date:%Y-%m-%d}'",
            self.sa_session.bind
        ).squeeze()
        return date_from_db


class TushareCrawlerJob(_BaseCrawlerJob):

    def __init__(self, job_id=None, execution_id=None, env='tushare_prod'):
        self.env = env
        super().__init__(job_id, execution_id)

    def get_tushare_data(self, api_name, date_cols=None, org_cols=None, col_mapping=None, **func_kwargs):
        func = getattr(get_tushare_api(self.env), api_name)
        result = func(**func_kwargs).fillna(np.nan)
        if date_cols:
            for c in date_cols:
                result.loc[:, c] = pd.to_datetime(result[c])
        if org_cols:
            result = result.filter(org_cols, axis=1)
        if col_mapping:
            result = result.rename(columns=col_mapping)
        return result


class WebCrawlerJob(_BaseCrawlerJob):

    def request(self, method, url):
        return request(
            method, url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/81.0.4044.138 Safari/537.36',
            }
        )

    def random_str(self, lenth):
        return ''.join(random.sample((chr(i) for i in (*range(65, 91), *range(97, 123))), lenth))
