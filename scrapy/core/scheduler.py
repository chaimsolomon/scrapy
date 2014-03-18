import os
import json
from os.path import join, exists

from queuelib import PriorityQueue
from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.misc import load_object
from scrapy.utils.job import job_dir
from scrapy import log

class Scheduler(object):

    def __init__(self, dupefilter, jobdir=None, rqclass=None, logunser=False, stats=None):
        self.df = dupefilter
        self.rqclass = rqclass
        self.logunser = logunser
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])
        dupefilter = dupefilter_cls.from_settings(settings)
        rqclass = load_object(settings['SCHEDULER_RABBIT_QUEUE'])
        logunser = settings.getbool('LOG_UNSERIALIZABLE_REQUESTS')
        return cls(dupefilter, job_dir(settings), rqclass, logunser, crawler.stats)

    def has_pending_requests(self):
        return len(self) > 0

    def open(self, spider):
        self.spider = spider
        self.rqs = self.rqclass(queuename=self.spider.__class__.__module__)
        return self.df.open()

    def close(self, reason):
        return self.df.close(reason)

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return
        self._rqpush(request)
        self.stats.inc_value('scheduler/enqueued/rabbit', spider=self.spider)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)

    def next_request(self):
        request = self.rqs.pop()
        if request:
            self.stats.inc_value('scheduler/dequeued/rabbit', spider=self.spider)
        if request:
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)
        return request

    def __len__(self):
        return len(self.rqs)

    def _rqpush(self, request):
        self.rqs.push(request)

    def _rqpop(self):
        if self.rqs:
            d = self.rqs.pop()
            if d:
                return request_from_dict(d, self.spider)
