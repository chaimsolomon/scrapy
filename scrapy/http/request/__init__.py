"""
This module implements the Request class which is used to represent HTTP
requests in Scrapy.

See documentation in docs/topics/request-response.rst
"""

import copy

from w3lib.url import safe_url_string

from scrapy.http.headers import Headers
from scrapy.utils.trackref import object_ref
from scrapy.utils.decorator import deprecated
from scrapy.utils.url import escape_ajax
from scrapy.http.common import obsolete_setter


class Request(object_ref):

    def __init__(self, url, callback=None, method='GET', headers=None, body=None, 
                 cookies=None, meta=None, encoding='utf-8', priority=0,
                 dont_filter=False, errback=None):

        self._encoding = encoding  # this one has to be set first
        self.method = str(method).upper()
        self._set_url(url)
        self._set_body(body)
        assert isinstance(priority, int), "Request priority not an integer: %r" % priority
        self.priority = priority

        assert callback or not errback, "Cannot use errback without a callback"
        self.callback = callback
        self.errback = errback

        self.cookies = cookies or {}
        self.headers = Headers(headers or {}, encoding=encoding)
        self.dont_filter = dont_filter

        self._meta = dict(meta) if meta else None

    @property
    def meta(self):
        if self._meta is None:
            self._meta = {}
        return self._meta

    def _get_url(self):
        return self._url

    def _set_url(self, url):
        if isinstance(url, str):
            self._url = escape_ajax(safe_url_string(url))
        elif isinstance(url, unicode):
            if self.encoding is None:
                raise TypeError('Cannot convert unicode url - %s has no encoding' %
                    type(self).__name__)
            self._set_url(url.encode(self.encoding))
        else:
            raise TypeError('Request url must be str or unicode, got %s:' % type(url).__name__)
        if ':' not in self._url:
            raise ValueError('Missing scheme in request url: %s' % self._url)

    url = property(_get_url, obsolete_setter(_set_url, 'url'))

    def _get_body(self):
        return self._body

    def _set_body(self, body):
        if isinstance(body, str):
            self._body = body
        elif isinstance(body, unicode):
            if self.encoding is None:
                raise TypeError('Cannot convert unicode body - %s has no encoding' %
                    type(self).__name__)
            self._body = body.encode(self.encoding)
        elif body is None:
            self._body = ''
        else:
            raise TypeError("Request body must either str or unicode. Got: '%s'" % type(body).__name__)

    body = property(_get_body, obsolete_setter(_set_body, 'body'))

    @property
    def encoding(self):
        return self._encoding

    def __str__(self):
        return "<%s %s>" % (self.method, self.url)

    __repr__ = __str__

    def __getstate__(self):
        state = {}
        state['url'] = self._get_url()
        state['method'] = self.method
        state['headers'] = self.headers
        state['cookies'] = self.cookies
        state['meta'] = self.meta
        state['encoding'] = self._encoding
        state['priority'] = self.priority
        state['dont_filter'] = self.dont_filter
        if self.callback is not None:
            state['callback_classpath'] = self.callback.im_self.__class__.__module__
            state['callback_classname'] = self.callback.im_self.__class__.__name__
            state['callback_funcname'] = self.callback.im_func.__name__
        return state

    def __setstate__(self, state):
        self._set_url(state['url'])
        self._meta = state['meta']
        self.method = state['method']
        self.headers = state['headers']
        self.cookies = state['cookies']
        if 'priority' in state:
            self.priority = state['priority']
        else:
            self.priority = 0
        if 'dont_filter' in state:
            self.dont_filter = state['dont_filter']
        else:
            self.dont_filter = False
        if 'encoding' in state:
            self._encoding = state['encoding']
        self._body = ''
        self.errback = None
        if 'callback_classpath' in state:
            module = __import__(state['callback_classpath'], globals(), locals(), [state['callback_classname']], -1)
            oclass = getattr(module, state['callback_classname'])
            obj = oclass()
            self.callback = getattr(obj, state['callback_funcname'])
        else:
            self.callback = None

    def copy(self):
        """Return a copy of this Request"""
        return self.replace()

    def replace(self, *args, **kwargs):
        """Create a new Request with the same attributes except for those
        given new values.
        """
        for x in ['url', 'method', 'headers', 'body', 'cookies', 'meta',
                  'encoding', 'priority', 'dont_filter', 'callback', 'errback']:
            kwargs.setdefault(x, getattr(self, x))
        cls = kwargs.pop('cls', self.__class__)
        return cls(*args, **kwargs)
