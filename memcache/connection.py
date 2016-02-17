from __future__ import (
    print_function,
    absolute_import,
)

import binascii
import io
import logging
import pickle
import re
import socket
import time
import zlib

import six

from . import (
    const,
    exc,
    utils,
)


class Connection(object):
    DEAD_RETRY = 30  # number of seconds before retrying a dead connection
    SOCKET_TIMEOUT = 3   # number of seconds before sockets timeout
    FLUSH_ON_RECONNECT = False

    COMPRESSOR = zlib.compress
    DECOMPRESSOR = zlib.decompress

    PICKLER = pickle.Pickler
    UNPICKLER = pickle.Unpickler
    PICKLE_PROTOCOL = 0

    def __init__(self, host, dead_retry=None, persistent_load=None,
                 socket_timeout=None, flush_on_reconnect=None,
                 persistent_id=None, cas_ids=None):
        self.dead_retry = dead_retry or self.DEAD_RETRY
        self.socket_timeout = socket_timeout or self.SOCKET_TIMEOUT
        self.flush_on_reconnect = flush_on_reconnect or self.FLUSH_ON_RECONNECT

        self.persistent_load = persistent_load
        self.persistent_id = persistent_id
        self.cas_ids = cas_ids

        if isinstance(host, tuple):
            host, self.weight = host
        else:
            self.weight = 1

        #  parse the connection string
        m = re.match(r'^(?P<proto>unix):(?P<path>.*)$', host)
        if not m:
            m = re.match(r'^(?P<proto>inet6):'
                         r'\[(?P<host>[^\[\]]+)\](:(?P<port>[0-9]+))?$', host)
        if not m:
            m = re.match(r'^(?P<proto>inet):'
                         r'(?P<host>[^:]+)(:(?P<port>[0-9]+))?$', host)
        if not m:
            m = re.match(r'^(?P<host>[^:]+)(:(?P<port>[0-9]+))?$', host)
        if not m:
            raise ValueError('Unable to parse connection string: "%s"' % host)

        host_data = m.groupdict()
        if host_data.get('proto') == 'unix':
            self.family = socket.AF_UNIX
            self.address = host_data['path']
        elif host_data.get('proto') == 'inet6':
            self.family = socket.AF_INET6
            self.ip = host_data['host']
            self.port = int(host_data.get('port') or 11211)
            self.address = (self.ip, self.port)
        else:
            self.family = socket.AF_INET
            self.ip = host_data['host']
            self.port = int(host_data.get('port') or 11211)
            self.address = (self.ip, self.port)

        self.deaduntil = 0
        self.socket = None
        self.flush_on_next_connect = 0

        self.buffer = b''
        self.logger = logging.getLogger('memcache.connection')

    def connect(self):
        if self.deaduntil and self.deaduntil > time.time():
            return
        self.deaduntil = 0

        if self.socket:
            return self.socket
        s = socket.socket(self.family, socket.SOCK_STREAM)
        if hasattr(s, 'settimeout'):
            s.settimeout(self.socket_timeout)
        try:
            s.connect(self.address)
        except socket.timeout as msg:
            self.mark_dead("connect: %s" % msg)
            return None
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            self.mark_dead("connect: %s" % msg)
            return None
        self.socket = s
        self.buffer = b''
        if self.flush_on_next_connect:
            self.flush()
            self.flush_on_next_connect = 0
        return s

    def mark_dead(self, reason):
        self.logger.debug("MemCache: %s: %s.  Marking dead." % (self, reason))
        self.deaduntil = time.time() + self.dead_retry
        if self.flush_on_reconnect:
            self.flush_on_next_connect = 1
        self.close()

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def send_one(self, command):
        if isinstance(command, six.text_type):
            command = command.encode('utf8')
        command = command + b'\r\n'
        self.send([command])

    def send(self, commands):
        """commands already has trailing \r\n's applied."""
        commands = b''.join(commands)
        if isinstance(commands, six.text_type):
            commands = commands.encode('utf8')
        self.socket.sendall(commands)

    def readline(self, raise_exception=False):
        """Read a line and return it.

        If "raise_exception" is set, raise exc.MemcachedConnectionDeadError if
        the read fails, otherwise return an empty string.
        """
        buf = self.buffer
        if self.socket:
            recv = self.socket.recv
        else:
            recv = lambda bufsize: b''

        while True:
            index = buf.find(b'\r\n')
            if index >= 0:
                break
            data = recv(4096)
            if not data:
                # connection close, let's kill it and raise
                self.mark_dead('connection closed in readline()')
                if raise_exception:
                    raise exc.MemcachedConnectionDeadError()
                else:
                    return ''

            buf += data
        self.buffer = buf[index + 2:]
        return buf[:index]

    def expect(self, text, raise_exception=False):
        line = self.readline(raise_exception)
        if line != text:
            if six.PY3:
                text = text.decode('utf8')
                log_line = line.decode('utf8', 'replace')
            else:
                log_line = line
            self.logger.debug("while expecting %r, got unexpected response %r"
                              % (text, log_line))
        return line

    def expect_cas_value(self, line=None, raise_exception=False):
        if not line:
            line = self.readline(raise_exception)

        if line and line[:5] == b'VALUE':
            resp, rkey, flags, len, cas_id = line.split()
            return (rkey, int(flags), int(len), int(cas_id))
        else:
            return (None, None, None, None)

    def expect_value(self, line=None, raise_exception=False):
        if not line:
            line = self.readline(raise_exception)

        if line and line[:5] == b'VALUE':
            resp, rkey, flags, len = line.split()
            flags = int(flags)
            rlen = int(len)
            return (rkey, flags, rlen)
        else:
            return (None, None, None)

    def recv(self, rlen):
        buf = self.buffer
        while len(buf) < rlen:
            foo = self.socket.recv(max(rlen - len(buf), 4096))
            buf += foo
            if not foo:
                raise exc.MemcachedError(
                    'Read %d bytes, expecting %d, read '
                    'returned 0 length bytes' % (len(buf), rlen))
        self.buffer = buf[rlen:]
        return buf[:rlen]

    def recv_value(self, flags, rlen):
        rlen += 2  # include \r\n
        buf = self.recv(rlen)
        if len(buf) != rlen:
            raise exc.MemcachedError(
                "received %d bytes when expecting %d" % (len(buf), rlen))

        if len(buf) == rlen:
            buf = buf[:-2]  # strip \r\n

        if flags & const.FLAG_COMPRESSED:
            buf = self.COMPRESSOR(buf)
            flags &= ~const.FLAG_COMPRESSED

        if flags == 0:
            # Bare string
            if six.PY3:
                val = buf.decode('utf8')
            else:
                val = buf
        elif flags & const.FLAG_INTEGER:
            val = int(buf)
        elif flags & const.FLAG_LONG:
            if six.PY3:
                val = int(buf)
            else:
                val = long(buf)
        elif flags & const.FLAG_PICKLE:
            try:
                file = io.BytesIO(buf)
                unpickler = self.UNPICKLER(file)
                if self.persistent_load:
                    unpickler.persistent_load = self.persistent_load
                val = unpickler.load()
            except Exception as e:
                self.logger.debug('Pickle error: %s\n' % e)
                return None
        else:
            self.logger.debug("unknown flags on get: %x\n" % flags)
            raise ValueError('Unknown flags on get: %x' % flags)

        return val

    def flush(self):
        self.send_one('flush_all')
        self.expect(b'OK')

    def convert_value(self, val, min_compress_len):
        """Transform val to a storable representation.

        Returns a tuple of the flags, the length of the new value, and
        the new value itself.
        """
        flags = 0
        if isinstance(val, six.binary_type):
            pass
        elif isinstance(val, six.text_type):
            val = val.encode('utf-8')
        elif isinstance(val, int):
            flags |= const.FLAG_INTEGER
            val = '%d' % val
            if six.PY3:
                val = val.encode('ascii')
            # force no attempt to compress this silly string.
            min_compress_len = 0
        elif six.PY2 and isinstance(val, long):
            flags |= const.FLAG_LONG
            val = str(val)
            if six.PY3:
                val = val.encode('ascii')
            # force no attempt to compress this silly string.
            min_compress_len = 0
        else:
            flags |= const.FLAG_PICKLE
            file = io.BytesIO()
            try:
                pickler = self.PICKLER(file, protocol=self.PICKLE_PROTOCOL)
            except TypeError:
                pickler = self.PICKLER(file, self.PICKLE_PROTOCOL)
            if self.persistent_id:
                pickler.persistent_id = self.persistent_id
            pickler.dump(val)
            val = file.getvalue()

        lv = len(val)
        # We should try to compress if min_compress_len > 0
        # and this string is longer than our min threshold.
        if min_compress_len and lv > min_compress_len:
            comp_val = self.COMPRESSOR(val)
            # Only retain the result if the compression result is smaller
            # than the original.
            if len(comp_val) < lv:
                flags |= const.FLAG_COMPRESSED
                val = comp_val

        #  silently do not store if value length exceeds maximum
        if (len(val) > const.MAX_VALUE_LENGTH):
            return(0)

        return (flags, len(val), val)

    def __str__(self):
        d = ''
        if self.deaduntil:
            d = " (dead until %d)" % self.deaduntil

        if self.family == socket.AF_INET:
            return "inet:%s:%d%s" % (self.address[0], self.address[1], d)
        elif self.family == socket.AF_INET6:
            return "inet6:[%s]:%d%s" % (self.address[0], self.address[1], d)
        else:
            return "unix:%s%s" % (self.address, d)

    def _deletetouch(self, cmd, key, expected, time=0, noreply=False):
        if time is not None and time != 0:
            headers = str(time)
        else:
            headers = None
        fullcmd = utils.encode_command(cmd, key, headers, noreply)

        try:
            self.send_one(fullcmd)
            if noreply:
                return 1
            line = self.readline()
            if line and line.strip() in expected:
                return 1
            self.logger.debug('%s expected %s, got: %r'
                              % (cmd, ' or '.join(expected), line))
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            self.mark_dead(msg)
        return 0

    def _incrdecr(self, cmd, key, delta, noreply=False):
        fullcmd = utils.encode_command(cmd, key, str(delta), noreply)
        try:
            self.send_one(fullcmd)
            if noreply:
                return
            line = self.readline()
            if line is None or line.strip() == b'NOT_FOUND':
                return None
            return int(line)
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            self.mark_dead(msg)
            return None

    def _unsafe_set(self, cmd, key, val, time, min_compress_len, noreply):
        if cmd == 'cas' and key not in self.cas_ids:
            return self._set('set', key, val, time, min_compress_len,
                             noreply)

        store_info = self.convert_value(val, min_compress_len)
        if not store_info:
            return(0)
        flags, len_val, encoded_val = store_info

        if cmd == 'cas':
            headers = ("%d %d %d %d"
                       % (flags, time, len_val, self.cas_ids[key]))
        else:
            headers = "%d %d %d" % (flags, time, len_val)
        fullcmd = utils.encode_command(cmd, key, headers, noreply,
                                       b'\r\n', encoded_val)

        try:
            self.send_one(fullcmd)
            if noreply:
                return True
            return(self.expect(b"STORED", raise_exception=True)
                   == b"STORED")
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            self.mark_dead(msg)
        return 0

    def _set(self, cmd, key, val, time, min_compress_len=0, noreply=False):
        try:
            return self._unsafe_set(cmd, key, val, time, min_compress_len,
                                    noreply)
        except exc.MemcachedConnectionDeadError:
            # retry once
            try:
                if self.connect():
                    return self._unsafe_set(cmd, key, val, time,
                                            min_compress_len, noreply,
                                            self)
            except (exc.MemcachedConnectionDeadError, socket.error) as msg:
                self.mark_dead(msg)
            return 0

    def _unsafe_get(self, cmd, key):
        try:
            cmd_bytes = cmd.encode('utf-8') if six.PY3 else cmd
            fullcmd = b''.join((cmd_bytes, b' ', key))
            self.send_one(fullcmd)
            rkey = flags = rlen = cas_id = None

            if cmd == 'gets':
                rkey, flags, rlen, cas_id, = \
                    self.expect_cas_value(raise_exception=True)
                if rkey and self.cache_cas:
                    self.cas_ids[rkey] = cas_id
            else:
                rkey, flags, rlen, = \
                    self.expect_value(raise_exception=True)

            if not rkey:
                return None
            try:
                value = self.recv_value(flags, rlen)
            finally:
                self.expect(b"END", raise_exception=True)
        except (exc.MemcachedError, socket.error) as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            self.mark_dead(msg)
            return None

        return value

    def _get(self, cmd, key):
        try:
            return self._unsafe_get(cmd, key)
        except exc.MemcachedConnectionDeadError:
            # retry once
            try:
                if self.connect():
                    return self._unsafe_get(cmd, key)
                return None
            except (exc.MemcachedConnectionDeadError, socket.error) as msg:
                self.mark_dead(msg)
            return None


class ConnectionPool(object):
    CONNECTION = Connection
    CONNECTION_RETRIES = 10  # how many times to try finding a free server.

    def __init__(self, connections, conn_settings):
        self.connection = [self.CONNECTION(c, **conn_settings)
                           for c in connections]
        self.buckets = []
        for conn in self.connection:
            self.buckets.extend([conn for c in range(conn.weight)])

    @classmethod
    def cmemcache_hash(cls, key):
        return ((((binascii.crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1)

    def get(self, key):
        if isinstance(key, tuple):
            conn_hash, key = key
        else:
            conn_hash = self.cmemcache_hash(key)

        if not self.buckets:
            return None, None

        for i in range(self.CONNECTION_RETRIES):
            conn = self.buckets[conn_hash % len(self.buckets)]
            if conn.connect():
                break
            conn_hash = str(conn_hash) + str(i)
            if isinstance(conn_hash, six.text_type):
                conn_hash = conn_hash.encode('ascii')
            conn_hash = self.cmemcache_hash(conn_hash)
        else:
            return None, None
        return conn, key

    def __iter__(self):
        for conn in self.connection:
            yield conn
