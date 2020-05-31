# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 23:32
@Author: Sue Zhu
"""
import logging
import random
from functools import lru_cache
from uuid import uuid4

import numpy as np
import pandas as pd
import sqlalchemy as sa
from requests import request
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from paramecium.database import get_session, get_dates
from paramecium.utils.data_api import get_tushare_api
from paramecium.scheduler_.scheduler import job


class _BaseDBJob(job.JobBase):
    meta_args = None  # tuple of dict with type and description, both string.
    meta_args_example = ''  # string, json like

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

    @classmethod
    def get_logger(cls):
        logger = logging.getLogger(cls.get_model_name())
        return logger

    @classmethod
    def get_session(cls):
        return get_session()

    def bulk_insert(self, records, model, msg=''):
        if isinstance(records, pd.DataFrame):
            records = [record.dropna().to_dict() for _, record in records.iterrows()]

        with self.get_session() as session:
            self.get_logger().info(f'start to bulk insert data {msg}...')
            session.bulk_insert_mappings(model, records)

    def upsert_data(self, records, model, ukeys=None, msg=''):
        result_ids = []

        if isinstance(records, pd.DataFrame):
            records = (record.dropna() for _, record in records.iterrows())

        with self.get_session() as session:
            self.get_logger().info(f'start to upsert data {msg}...')
            for record in records:
                insert_exe = insert(model).values(**record)
                if ukeys:
                    set_ = {k: sa.text(f'EXCLUDED.{k}') for k in {*record.keys()} - {*(c.key for c in ukeys)}}
                    insert_exe = insert_exe.on_conflict_do_update(
                        index_elements=ukeys, set_={**set_, 'updated_at': sa.func.current_timestamp()}
                    )
                exe_result = session.execute(insert_exe)
                result_ids.extend(exe_result.inserted_primary_key)

        return result_ids

    def clean_duplicates(self, model, unique_cols):
        self.get_logger().debug(f'Clean duplicate data after bulk insert.')
        with self.get_session() as session:
            labeled_cols = [c.label(c.key) for c in unique_cols]
            pk, *_ = model.get_primary_key()
            grouped = Query(labeled_cols).group_by(*unique_cols).having(sa.func.count(pk) > 1).subquery('g')
            duplicates = pd.DataFrame(
                session.query(pk, *labeled_cols).join(
                    grouped, sa.and_(*(grouped.c[c.key] == c for c in unique_cols))
                ).order_by(model.updated_at).all()
            ).set_index(pk.name)
            if not duplicates.empty:
                session.query(model).filter(pk.in_(
                    duplicates.loc[lambda df: df.duplicated(keep='last')].index.tolist()
                )).delete(synchronize_session='fetch')

    @staticmethod
    @lru_cache()
    def get_dates(freq=None):
        with _BaseDBJob.get_session() as session:
            dates = get_dates(freq)
        return dates

    @property
    def last_td_date(self):
        cur_date = pd.Timestamp.now()
        if cur_date.hour <= 22:
            cur_date -= pd.Timedelta(days=1)
        return max((t for t in self.get_dates('D') if t <= cur_date))


class TushareCrawlerJob(_BaseDBJob):

    def __init__(self, job_id=None, execution_id=None, env='tushare_prod'):
        self.env = env
        super().__init__(job_id, execution_id)

    def get_tushare_data(self, api_name, date_cols=None, fields=None, col_mapping=None, **func_kwargs):
        api = get_tushare_api(self.env)
        result = api.query(api_name, **func_kwargs, fields=','.join(fields) if fields else '').fillna(np.nan)
        if date_cols:
            for c in date_cols:
                result.loc[:, c] = pd.to_datetime(result[c])
        # if org_cols:
        #     result = result.filter(org_cols, axis=1)
        if col_mapping:
            result = result.rename(columns=col_mapping)
        return result


class WebCrawlerJob(_BaseDBJob):

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
