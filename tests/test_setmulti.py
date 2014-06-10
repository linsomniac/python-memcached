#!/usr/bin/env python
#
#  Tests for set_multi.
#
# ==============
#  This is based on a skeleton test file, more information at:
#
#     https://github.com/linsomniac/python-unittest-skeleton

from __future__ import print_function

import socket
import sys
import unittest

sys.path.append('..')
import memcache

DEBUG = False


class test_Memcached_Set_Multi(unittest.TestCase):
    def setUp(self):
        RECV_CHUNKS = ['chunk1']

        class FakeSocket(object):
            def __init__(self, *args):
                if DEBUG:
                    print('FakeSocket{0!r}'.format(args))
                self._recv_chunks = list(RECV_CHUNKS)

            def connect(self, *args):
                if DEBUG:
                    print('FakeSocket.connect{0!r}'.format(args))

            def sendall(self, *args):
                if DEBUG:
                    print('FakeSocket.sendall{0!r}'.format(args))

            def recv(self, *args):
                if self._recv_chunks:
                    data = self._recv_chunks.pop(0)
                else:
                    data = ''
                if DEBUG:
                    print('FakeSocket.recv{0!r} -> {1!r}'.format(args, data))
                return data

            def close(self):
                if DEBUG:
                    print('FakeSocket.close()')

        self.old_socket = socket.socket
        socket.socket = FakeSocket

    def tearDown(self):
        socket.socket = self.old_socket

    def test_Socket_Disconnect(self):
        client = memcache.Client(['memcached'], debug=True)
        mapping = {'foo': 'FOO', 'bar': 'BAR'}
        bad_keys = client.set_multi(mapping)

        self.assertEqual(sorted(bad_keys), ['bar', 'foo'])

        if DEBUG:
            print('set_multi({0!r}) -> {1!r}'.format(mapping, bad_keys))

if __name__ == '__main__':
    unittest.main()
