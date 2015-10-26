#!/usr/bin/env python
# coding:utf-8

from tornado.gen import coroutine, Return
from tornado.tcpclient import TCPClient
import binascii
from collections import defaultdict
import zlib

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

try:
    import cPickle as pickle
except ImportError:
    import pickle

_FLAG_PICKLE = 1 << 0
_FLAG_INTEGER = 1 << 1
_FLAG_LONG = 1 << 2
_FLAG_COMPRESSED = 1 << 3


class TCPConnection(TCPClient):
    """A connection module to handle single TCP connection with non-block socket.

    Each memcached client may have multi hosts,
    each host should hold their own connection with remote server.
    """
    def __init__(self, host, port, debug=False, resolver=None, io_loop=None):
        super(TCPConnection, self).__init__(resolver, io_loop)
        self.host = host
        self.port = port
        self.debug = debug
        self.stream = None

    @coroutine
    def get_connection(self):
        """Return the stream of current IOStream if connected, else make a new connection and return the stream.

        """
        if not self.stream or self.stream.closed():
            self.stream = yield self.connect(self.host, self.port)
            self.stream.debug = self.debug
        raise Return(self.stream)

    @coroutine
    def send_cmd(self, cmd):
        """Send one command to remote server each time.

        Use read_one_line(), read_bytes() to get the result from the stream.
        :param cmd: the cmd client send to remote server, like "get key"
        :return:
        """
        cmd += '\r\n'.encode()
        stream = yield self.get_connection()
        yield stream.write(cmd)
        raise Return()

    @coroutine
    def read_one_line(self):
        response = yield self.stream.read_until(b'\r\n')
        raise Return(response.strip('\r\n'))

    @coroutine
    def read_bytes(self, length=1024):
        response = yield self.stream.read_bytes(length)
        raise Return(response)

    def close(self):
        if self.stream:
            self.stream = None
            super(TCPConnection, self).close()


class Connection(object):
    """Async memcached client.

    We can handle one connection with multi hosts like memcache.py by using this module.
    With the help of tornado, we can use this handle the operation like this::


        from connection import Connection
        from tornado.gen import coroutine, Return

        conn = Connection(['127.0.0.1:11211'])

        @coroutine
        def sample():
            yield conn.set("key", "sample", 60)
            res = yield conn.get("key")
            print res
            yield conn.set_multi({"key1": "sample1", "key2": "sample2"}, 60, key_prefix="multi_")
            yield conn.delete("key1")
            res_multi = yield conn.get_multi(["key", "key1", "key2", "key3"])
            print res_multi

        sample()


    the output should be:

        sample
        {"key": "sample", "key2": "sample2"}


    """
    def __init__(self, servers, debug=False, op_times=0):
        """Single memcached connection with multi hosts

        :param servers: servers is a list contains host:port strings like ["192.168.2.3:11211", "192.168.2.33:11211"]
        :param debug: debug=True cause logging.level=logging.DEBUG
        :param op_times: record how many times this connection has been used,
                        increased by connection user(For example Pool)
        :return:
        """
        assert isinstance(servers, list)
        self.op_times = op_times
        servers = [_.split(":") for _ in servers]
        self.hosts = [TCPConnection(_[0], _[1], debug) for _ in servers]

    def _cmemcache_hash(self, key):
        """copy from memcache.py

        """
        return (
            (((binascii.crc32(key.encode('ascii')) & 0xffffffff) >> 16) & 0x7fff) or 1
        )

    def _convert_key(self, key_list=None, key=None):
        out_key = list()
        if not key_list and key:
            if not isinstance(key, bytes):
                key =key.encode('utf-8')
            return key
        for k in key_list:
            if not isinstance(k, bytes):
                k = k.encode('utf-8')
            out_key.append(k)
        return out_key

    def _convert_recv_value(self, flags, value):
        if flags & _FLAG_COMPRESSED:
            value = zlib.decompress(value)
        if flags == 0 or flags == _FLAG_COMPRESSED:
            return value
        elif flags & _FLAG_INTEGER:
            return int(value)
        elif flags & _FLAG_LONG:
            return long(value)
        elif flags & _FLAG_PICKLE:
            try:
                f = StringIO(value)
                unpickler = pickle.Unpickler(f)
                return unpickler.load()
            except Exception, e:
                pass
        return ""

    def _map_and_prefix_keys(self, key_iterable, key_prefix):
        pass

    def _make_host_keys_pare(self, keys, key_prefix):
        key_list = [key_prefix+str(key) for key in keys]
        host_key_dict = defaultdict(list)
        for key in key_list:
            host = self.get_host(key)
            host_key_dict[host].append(key)
        return host_key_dict

    def _get_store_info(self, value, min_compress_len):
        flags = 0
        if isinstance(value, unicode):
            value = value.encode('utf-8')
            min_compress_len = 0
        elif isinstance(value, str):
            pass
        elif isinstance(value, int):
            flags |= _FLAG_INTEGER
            value = "%d" % value
            min_compress_len = 0
        elif isinstance(value, long):
            flags |= _FLAG_LONG
            value = "%d" % value
        else:
            flags |= _FLAG_PICKLE
            f = StringIO()
            pickler = pickle.Pickler(f)
            pickler.dump(value)
            value = f.getvalue()
        lv = len(value)
        if min_compress_len and lv > min_compress_len:
            comp_val = zlib.compress(value)
            if len(comp_val) < lv:
                flags |= _FLAG_COMPRESSED
                value = comp_val
        return flags, value

    @coroutine
    def get_stream(self, cmd):
        host = self.hosts[self._cmemcache_hash(cmd) % len(self.hosts)]
        stream = yield host.get_connection()
        raise Return(stream)

    def get_host(self, key):
        return self.hosts[self._cmemcache_hash(key) % len(self.hosts)]

    def disconnect_all(self):
        for _ in self.hosts:
            _.close()

    @coroutine
    def _get(self, cmd, key):
        host = self.get_host(key)
        command = "%s %s" % (cmd, key)
        yield host.send_cmd(command)
        response = yield host.read_one_line()
        if response == 'END':
            raise Return(None)
        if cmd == 'gets':
            _, _, flags, length, cas_id = response.split(' ')
        else:
            _, _, flags, length = response.split(' ')
        flags = int(flags)
        length = int(length) + 2  # with '\r\n'
        value = yield host.read_bytes(length)
        end = yield host.read_one_line()
        assert end == 'END'

        result = self._convert_recv_value(flags, value[:-2])
        if cmd == 'gets':
            result = (result, int(cas_id))
        raise Return(result)

    @coroutine
    def get(self, key):
        response = yield self._get('get', key)
        raise Return(response)

    @coroutine
    def gets(self, key):
        response = yield self._get('gets', key)
        raise Return(response)

    @coroutine
    def get_multi(self, keys, key_prefix=""):
        response = dict()
        orig_to_no_prefix = dict((key_prefix + str(key), key) for key in keys)  # used when return the dict value
        host_keys_dict = self._make_host_keys_pare(keys, key_prefix)
        for host, key_list in host_keys_dict.iteritems():
            command = "get %s" % (' '.join(key_list))
            yield host.send_cmd(command)
            line = yield host.read_one_line()
            while line and line != "END":
                _, key, flags, length = line.split(" ")
                length = int(length) + 2  # include '\r\n'
                flags = int(flags)
                value = yield host.read_bytes(length)
                value = value[:-2]  # read value and split '\r\n'
                response[orig_to_no_prefix[key]] = self._convert_recv_value(flags, value)
                line = yield host.read_one_line()
        raise Return(response)

    @coroutine
    def _set(self, cmd, key, val, time=0, min_compress_len=0):
        flags, value = self._get_store_info(val, min_compress_len)
        host = self.get_host(key)
        command = "%s %s %d %d %d\r\n%s" % (cmd, key, flags, time, len(value), value)
        yield host.send_cmd(command)
        response = yield host.read_one_line()
        raise Return(response == 'STORED')

    @coroutine
    def set(self, key, val, time, min_compress_len=0):
        result = yield self._set("set", key, val, time, min_compress_len)
        raise Return(result)

    @coroutine
    def add(self, key, val, time, min_compress_len=0):
        result = yield self._set("add", key, val, time, min_compress_len)
        raise Return(result)

    @coroutine
    def append(self, key, val, time, min_compress_len=0):
        result = yield self._set("append", key, val, time, min_compress_len)
        raise Return(result)

    @coroutine
    def prepend(self, key, val, time, min_compress_len=0):
        result = yield self._set("prepend", key, val, time, min_compress_len)
        raise Return(result)

    @coroutine
    def replace(self, key, val, time, min_compress_len=0):
        result = yield self._set("replace", key, val, time, min_compress_len)
        raise Return(result)

    @coroutine
    def cas(self, key, val, time, min_compress_len=0):
        result = yield self._set("cas", key, val, time, min_compress_len)
        raise Return(result)

    @coroutine
    def set_multi(self, mapping, time, key_prefix="", min_compress_len=0):
        failed_list = list()
        for k, value in mapping.iteritems():
            key = key_prefix + str(k)
            flags, value = self._get_store_info(value, min_compress_len)
            host = self.get_host(key)
            command = "%s %s %d %d %d\r\n%s" % ('set', key, flags, time, len(value), value)
            yield host.send_cmd(command)
            response = yield host.read_one_line()
            if response != 'STORED':
                failed_list.append(k)
        raise Return(failed_list)

    @coroutine
    def delete(self, key, time=0):
        host = self.get_host(key)
        command = "delete %s" % key
        yield host.send_cmd(command)
        response = yield host.read_one_line()
        raise Return(response in ('DELETED', 'NOT_FOUND'))

    @coroutine
    def delete_multi(self, keys, time=0, key_prefix=""):
        pass

    @coroutine
    def incr(self, key, delta=1):
        result = yield self._incrdecr("incr", key, delta)
        raise Return(result)

    @coroutine
    def decr(self, key, delta=1):
        result = yield self._incrdecr("decr", key, delta)
        raise Return(result)

    @coroutine
    def _incrdecr(self, cmd, key, delta):
        host = self.get_host(key)
        command = "%s %s %s" % (cmd, key, delta)
        yield host.send_cmd(command)
        response = yield host.read_one_line()
        if not response.isdigit():
            raise Return(None)
        raise Return(int(response))
