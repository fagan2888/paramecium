# -*- coding: utf-8 -*-
"""
@Time: 2020/5/9 23:32
@Author:  MUYUE1
"""
import json
import random
import re
from requests import request
import pandas as pd
from ndscheduler.corescheduler import job
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from paramecium.database import ts_api, get_engine
import sqlalchemy as sa

_HEADER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/81.0.4044.138 Safari/537.36'
}


class TushareCrawlerJob(job.JobBase):
    api = ts_api

    @property
    def model(self):
        return None

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
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            'notes': cls.__doc__,
        }

    def upsert_data(self, records, ukeys):
        Session = sessionmaker(bind=get_engine())
        session = Session()
        try:
            for record in records:
                session.execute(insert(self.model).values(
                    **record, updated_at=sa.text('current_timestamp')
                ).on_conflict_do_update(
                    index_elements=ukeys,
                    set_={k:sa.text(f'EXCLUDED.{k}') for k in record.keys() if k not in (c.key for c in ukeys)}
                ))
        except Exception as e:
            print(e)
            session.rollback()
        else:
            session.commit()



class WebCrawlerJob(job.JobBase):

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
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            'notes': cls.__doc__,
        }

    def comment_respond(self, method, url):
        return request(method, url, headers=_HEADER)

    def random_str(self, lenth):
        return ''.join(random.sample((chr(i) for i in (*range(65, 91), *range(97, 123))), lenth))
