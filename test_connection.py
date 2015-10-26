#!/usr/bin/env python
# coding:utf-8
#
# use `python -m tornado.testing test_connection` to run test cases

from tornado.testing import gen_test, AsyncTestCase
from connection import Connection, TCPConnection


class ConnectionTest(AsyncTestCase):
    def setUp(self):
        super(ConnectionTest, self).setUp()
        self.conn = Connection(['127.0.0.1:11211'], debug=True)

    def test_get_host(self):
        host = self.conn.get_host('key')
        assert isinstance(host, TCPConnection)

    @gen_test
    def test_set_get(self):
        res = yield self.conn.set('key', 'test', 10)
        assert res is True
        res = yield self.conn.get('key')
        assert res == 'test'

    @gen_test
    def test_delete(self):
        yield self.conn.set('del', 'del', 10)
        res = yield self.conn.get('del')
        assert res == 'del'
        res = yield self.conn.delete('del')
        assert res is True
        res = yield self.conn.get('del')
        assert res is None

    @gen_test
    def test_set_get_multi(self):
        res = yield self.conn.set_multi({'key1': 'test1', 'key2': 'test2', 'key3': 'test3', 1: '3', 2: 2}, 10, key_prefix="multi_")
        assert res == []
        res = yield self.conn.get_multi(['key1', 'key2', 'key3', 1, 2], key_prefix='multi_')
        print res
        assert res == {'key1': 'test1', 'key2': 'test2', 'key3': 'test3', 1: '3', 2: 2}
        res = yield self.conn.get_multi(['key', 'key2', 'key3'], key_prefix='multi_')
        print res
        assert res == {'key2': 'test2', 'key3': 'test3'}

    @gen_test
    def test_incr(self):
        yield self.conn.set('incr', 0, 10)
        res = yield self.conn.incr('incr')
        print 'incr0:', res
        res = yield self.conn.incr('incr')
        print 'incr1:', res

    @gen_test
    def test_close(self):
        self.conn.disconnect_all()

