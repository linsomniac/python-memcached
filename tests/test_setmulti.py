#!/usr/bin/env python
#
#  Tests for set_multi.
#
# ==============
#  This is based on a skeleton test file, more information at:
#
#     https://github.com/linsomniac/python-unittest-skeleton


import socket
import sys
import unittest

from .utils import captured_stderr

sys.path.append('..')
import memcache    # noqa: E402

DEBUG = False


class test_Memcached_Set_Multi(unittest.TestCase):
    def setUp(self):
        RECV_CHUNKS = [b'chunk1']

        class FakeSocket:
            def __init__(self, *args):
                if DEBUG:
                    print(f'FakeSocket{args!r}')
                self._recv_chunks = list(RECV_CHUNKS)

            def connect(self, *args):
                if DEBUG:
                    print(f'FakeSocket.connect{args!r}')

            def sendall(self, *args):
                if DEBUG:
                    print(f'FakeSocket.sendall{args!r}')

            def recv(self, *args):
                if self._recv_chunks:
                    data = self._recv_chunks.pop(0)
                else:
                    data = ''
                if DEBUG:
                    print(f'FakeSocket.recv{args!r} -> {data!r}')
                return data

            def close(self):
                if DEBUG:
                    print('FakeSocket.close()')

        self.old_socket = socket.socket
        socket.socket = FakeSocket

        self.mc = memcache.Client(['memcached'], debug=True)

    def tearDown(self):
        socket.socket = self.old_socket

    def test_Socket_Disconnect(self):
        mapping = {'foo': 'FOO', 'bar': 'BAR'}
        with captured_stderr() as log:
            bad_keys = self.mc.set_multi(mapping)
        self.assertIn('connection closed in readline().', log.getvalue())
        self.assertEqual(sorted(bad_keys), ['bar', 'foo'])
        if DEBUG:
            print(f'set_multi({mapping!r}) -> {bad_keys!r}')


if __name__ == '__main__':
    unittest.main()
