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

from paramecium.database import ts_api, get_engine

_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/81.0.4044.138 Safari/537.36'
}


class _BaseCrawlerJob(job.JobBase):

    def __init__(self, job_id=None, execution_id=None):
        if job_id is None:
            job_id = uuid4()
        if execution_id is None:
            execution_id = uuid4()
        super().__init__(job_id, execution_id)

    @classmethod
    def meta_info(cls):
        """
        NEED FILL KEY BELOW
        'arguments': [
            # is_update
            {'type': 'int', 'description': '1: update mode, 0: replace mode'},

            # argument2
            {'type': 'string', 'description': 'Second argument'}
        ],
        'example_arguments': '[1, ]'
        """
        return {
            'job_class_string': f'{cls.__module__:s}.{cls.__name__:s}',
            'notes': cls.__doc__,
        }

    @property
    def sa_session(self):
        Session = sessionmaker(bind=get_engine())
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
    api = ts_api


class WebCrawlerJob(_BaseCrawlerJob):

    def request(self, method, url):
        return request(method, url, headers=_HEADER)

    def random_str(self, lenth):
        return ''.join(random.sample((chr(i) for i in (*range(65, 91), *range(97, 123))), lenth))
