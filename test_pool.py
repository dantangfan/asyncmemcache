#!/usr/bin/env python
# coding:utf-8
#
# use `python -m tornado.testing test_pool` to run test cases


from tornado.testing import gen_test, AsyncTestCase
from pool import Pool


class PoolTest(AsyncTestCase):
    def setUp(self):
        super(PoolTest, self).setUp()
        self.pool = Pool(['127.0.0.1:11211', '127.0.0.1:11212'], debug=True)

    @gen_test
    def test_set_get_multi(self):
        res = yield self.pool.set_multi({'key1': 'test1', 'key2': 'test2', 'key3': 'test3', 1: '3', 2: 2}, 10, key_prefix="multi_")
        print res
        assert res == []
        res = yield self.pool.get_multi(['key1', 'key2', 'key3', 1, 2], key_prefix='multi_')
        print res
        assert res == {'key1': 'test1', 'key2': 'test2', 'key3': 'test3', 1: '3', 2: 2}
        yield self.pool.delete("multi_key1")
        res = yield self.pool.get_multi(['key1', 'key2', 'key3'], key_prefix='multi_')
        print res
        assert res == {'key2': 'test2', 'key3': 'test3'}

        print "--------------"
        yield self.pool.set_multi({"list": ['a', 'b', 2], "dict": {1: 'a', 'c': 'that is fun', 'info': {1: 3}}}, 10, key_prefix="multi_")
        res = yield self.pool.get_multi(['list', 'dict'], key_prefix="multi_")
        print res
        print type(res)