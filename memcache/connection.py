from __future__ import (
    print_function,
    absolute_import,
)

import re
import socket
import sys
import time

import six

from . import exc

DEAD_RETRY = 30  # number of seconds before retrying a dead server.
SOCKET_TIMEOUT = 3  # number of seconds before sockets timeout.


class Connection(object):
    def __init__(self, host, debug=0, dead_retry=DEAD_RETRY,
                 socket_timeout=SOCKET_TIMEOUT, flush_on_reconnect=0):
        self.dead_retry = dead_retry
        self.socket_timeout = socket_timeout
        self.debug = debug
        self.flush_on_reconnect = flush_on_reconnect
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

        hostData = m.groupdict()
        if hostData.get('proto') == 'unix':
            self.family = socket.AF_UNIX
            self.address = hostData['path']
        elif hostData.get('proto') == 'inet6':
            self.family = socket.AF_INET6
            self.ip = hostData['host']
            self.port = int(hostData.get('port') or 11211)
            self.address = (self.ip, self.port)
        else:
            self.family = socket.AF_INET
            self.ip = hostData['host']
            self.port = int(hostData.get('port') or 11211)
            self.address = (self.ip, self.port)

        self.deaduntil = 0
        self.socket = None
        self.flush_on_next_connect = 0

        self.buffer = b''

    def debuglog(self, str):
        if self.debug:
            sys.stderr.write("MemCached: %s\n" % str)

    def _check_dead(self):
        if self.deaduntil and self.deaduntil > time.time():
            return 1
        self.deaduntil = 0
        return 0

    def connect(self):
        if self._get_socket():
            return 1
        return 0

    def mark_dead(self, reason):
        self.debuglog("MemCache: %s: %s.  Marking dead." % (self, reason))
        self.deaduntil = time.time() + self.dead_retry
        if self.flush_on_reconnect:
            self.flush_on_next_connect = 1
        self.close_socket()

    def _get_socket(self):
        if self._check_dead():
            return None
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

    def close_socket(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def send_cmd(self, cmd):
        if isinstance(cmd, six.text_type):
            cmd = cmd.encode('utf8')
        self.socket.sendall(cmd + b'\r\n')

    def send_cmds(self, cmds):
        """cmds already has trailing \r\n's applied."""
        if isinstance(cmds, six.text_type):
            cmds = cmds.encode('utf8')
        self.socket.sendall(cmds)

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
        if self.debug and line != text:
            if six.PY3:
                text = text.decode('utf8')
                log_line = line.decode('utf8', 'replace')
            else:
                log_line = line
            self.debuglog("while expecting %r, got unexpected response %r"
                          % (text, log_line))
        return line

    def recv(self, rlen):
        self_socket_recv = self.socket.recv
        buf = self.buffer
        while len(buf) < rlen:
            foo = self_socket_recv(max(rlen - len(buf), 4096))
            buf += foo
            if not foo:
                raise exc.MemcachedError(
                    'Read %d bytes, expecting %d, read '
                    'returned 0 length bytes' % (len(buf), rlen))
        self.buffer = buf[rlen:]
        return buf[:rlen]

    def flush(self):
        self.send_cmd('flush_all')
        self.expect(b'OK')

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
