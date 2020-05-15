# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 23:32
@Author: Sue Zhu
"""
import random
from uuid import uuid4

import sqlalchemy as sa
from ndscheduler.corescheduler import job
from requests import request
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from paramecium.tools.db_source import get_tushare_api, get_sql_engine


class _BaseCrawlerJob(job.JobBase):
    meta_args = None  # tuple of dict with type and description, both string.
    meta_args_example = ''  # string, json like

    def __init__(self, job_id=None, execution_id=None):
        if job_id is None:
            job_id = uuid4()
        if execution_id is None:
            execution_id = uuid4()
        super().__init__(job_id, execution_id)

    @classmethod
    def meta_info(cls):
        """ 参数列表 """
        return {
            'job_class_string': f'{cls.__module__:s}.{cls.__name__:s}',
            'notes': cls.__doc__,
            'arguments': list(cls.meta_args) if cls.meta_args is not None else [],
            'example_arguments': cls.meta_args_example
        }

    @property
    def sa_session(self):
        Session = sessionmaker(bind=get_sql_engine(env='postgres'))
        return Session()

    def upsert_data(self, records, model, ukeys=None):
        session = self.sa_session
        try:
            for record in records:
                insert_exe = insert(model).values(**record, updated_at=sa.text('current_timestamp'))
                if ukeys:
                    insert_exe = insert_exe.on_conflict_do_update(
                        index_elements=ukeys,
                        set_={k: sa.text(f'EXCLUDED.{k}') for k in record.keys() if k not in (c.key for c in ukeys)}
                    )
                session.execute(insert_exe)
        except Exception as e:
            print(e)
            session.rollback()
        else:
            session.commit()


class TushareCrawlerJob(_BaseCrawlerJob):

    def __init__(self, job_id=None, execution_id=None, env='tushare_prod'):
        self.env = env
        super().__init__(job_id, execution_id)

    @property
    def api(self):
        return get_tushare_api(self.env)


class WebCrawlerJob(_BaseCrawlerJob):

    def request(self, method, url):
        return request(
            method, url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/81.0.4044.138 Safari/537.36',
            }
        )

    def random_str(self, lenth):
        return ''.join(random.sample((chr(i) for i in (*range(65, 91), *range(97, 123))), lenth))
