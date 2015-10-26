#!/usr/bin/env python
# coding:utf-8

"""Pool module for async memcached

We can handle a collection poll with this module.
This should give you a feel for how this module operate::

        from pool import Pool
        from tornado.gen import coroutine, Return

        pool = Pool(['127.0.0.1:11211'])

        @coroutine
        def sample():
            yield pool.set_multi({"key1": "sample1", "key2": "sample2"}, 60, key_prefix="multi_")
            res_multi = yield pool.get_multi(["key", "key1", "key2", "key3"])
            print res_multi
            yield pool.delete("key1")
            res_multi = yield pool.get_multi(["key", "key1", "key2", "key3"])
            print res_multi

        sample()


the output should be:

        {"key2": "sample2"}
        {"key1": "sample1", "key2": "sample2"}


"""

from tornado.gen import coroutine, Return, sleep
from collections import deque
import logging
from functools import wraps
from connection import Connection


class ConnectException(Exception):
    pass


class Pool(object):
    def __init__(self, servers, max_connection=20, flood_gate=10000, debug=False):
        """

        :param servers: a list of hosts like ['127.0.0.1:11211', '127.0.0.1:11212']
        :param max_connection:
        :param flood_gate: once 'op_times' of a connection reaches the flood_gate, this connection will be closed
        :param debug:
        :return:
        """
        self.__servers = servers
        self.__debug = debug
        self.__max_connection = max_connection
        self.__flood_gate = flood_gate
        self.__idle_queue = deque()
        self.__busy_queue = deque()

    def disconnect_all(self):
        for _ in self.__busy_queue:
            _.disconnect_all()
        for _ in self.__idle_queue:
            _.disconnect_all()

    @coroutine
    def __get_connection(self):
        if self.__idle_queue:
            conn = self.__idle_queue.popleft()
            self.__busy_queue.append(conn)
            raise Return(conn)
        elif len(self.__busy_queue) + len(self.__idle_queue) < self.__max_connection:
            conn = Connection(self.__servers, self.__debug)
            self.__busy_queue.append(conn)
            raise Return(conn)
        else:
            yield sleep(0.001)
            conn = yield self.__get_connection()
            raise Return(conn)

    def __shrink(self, conn):
        if conn.op_times > self.__flood_gate:
            conn.disconnect_all()
            conn = None
            return True
        return False

    def __recycle(self, conn):
        try:
            self.__busy_queue.remove(conn)
            if not self.__shrink(conn):
                self.__idle_queue.append(conn)
        except Exception as e:
            logging.warning("something wrong with server " + e)

    @coroutine
    def __do_cmd(self, method, *args, **kwargs):
        conn = yield self.__get_connection()
        if hasattr(conn, method):
            result = yield getattr(conn, method)(*args, **kwargs)
        else:
            logging.error("no such method")
            return
        self.__recycle(conn)
        raise Return(result)

    def __make_cmd_operation(name):
        def _(func):
            @wraps(func)
            @coroutine
            def cmd_op(cls, *args, **kwargs):
                result = yield cls.__do_cmd(name, *args, **kwargs)
                raise Return(result)
            return cmd_op
        return _

    @__make_cmd_operation('get_multi')
    def get_multi(self):
        pass

    @__make_cmd_operation('set_multi')
    def set_multi(self):
        pass

    @__make_cmd_operation('delete')
    def delete(self):
        pass
