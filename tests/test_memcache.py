# -*- coding: utf-8 -*-
from __future__ import print_function

import unittest
import zlib

try:
    import unittest.mock as mock
except ImportError:
    import mock

from memcache import Client, _Host, SERVER_MAX_KEY_LENGTH, SERVER_MAX_VALUE_LENGTH  # noqa: H301
from .utils import captured_stderr


class FooStruct(object):

    def __init__(self):
        self.bar = "baz"

    def __str__(self):
        return "A FooStruct"

    def __eq__(self, other):
        if isinstance(other, FooStruct):
            return self.bar == other.bar
        return 0


class TestMemcache(unittest.TestCase):
    def setUp(self):
        # TODO(): unix socket server stuff
        servers = ["127.0.0.1:11211"]
        self.mc = Client(servers, debug=1)

    def tearDown(self):
        self.mc.flush_all()
        self.mc.disconnect_all()

    def check_setget(self, key, val, noreply=False):
        self.mc.set(key, val, noreply=noreply)
        newval = self.mc.get(key)
        self.assertEqual(newval, val)

    def test_setget(self):
        self.check_setget("a_string", "some random string")
        self.check_setget("a_string_2", "some random string", noreply=True)
        self.check_setget("an_integer", 42)
        self.check_setget("an_integer_2", 42, noreply=True)

    def test_delete(self):
        self.check_setget("long", int(1 << 30))
        result = self.mc.delete("long")
        self.assertEqual(result, True)
        self.assertEqual(self.mc.get("long"), None)

    @mock.patch.object(_Host, 'send_cmd')
    @mock.patch.object(_Host, 'readline')
    def test_touch(self, mock_readline, mock_send_cmd):
        with captured_stderr():
            self.mc.touch('key')
        mock_send_cmd.assert_called_with(b'touch key 0')

    def test_get_multi(self):
        self.check_setget("gm_a_string", "some random string")
        self.check_setget("gm_an_integer", 42)
        self.assertEqual(
            self.mc.get_multi(["gm_a_string", "gm_an_integer"]),
            {"gm_an_integer": 42, "gm_a_string": "some random string"})

    def test_get_unknown_value(self):
        self.mc.delete("unknown_value")

        self.assertEqual(self.mc.get("unknown_value"), None)

    def test_setget_foostruct(self):
        f = FooStruct()
        self.check_setget("foostruct", f)
        self.check_setget("foostruct_2", f, noreply=True)

    def test_incr(self):
        self.check_setget("i_an_integer", 42)
        self.assertEqual(self.mc.incr("i_an_integer", 1), 43)

    def test_incr_noreply(self):
        self.check_setget("i_an_integer_2", 42)
        self.assertEqual(self.mc.incr("i_an_integer_2", 1, noreply=True), None)
        self.assertEqual(self.mc.get("i_an_integer_2"), 43)

    def test_decr(self):
        self.check_setget("i_an_integer", 42)
        self.assertEqual(self.mc.decr("i_an_integer", 1), 41)

    def test_decr_noreply(self):
        self.check_setget("i_an_integer_2", 42)
        self.assertEqual(self.mc.decr("i_an_integer_2", 1, noreply=True), None)
        self.assertEqual(self.mc.get("i_an_integer_2"), 41)

    def test_sending_spaces(self):
        try:
            self.mc.set("this has spaces", 1)
        except Client.MemcachedKeyCharacterError as err:
            self.assertTrue("characters not allowed" in err.args[0])
        else:
            self.fail(
                "Expected Client.MemcachedKeyCharacterError, nothing raised")

    def test_sending_control_characters(self):
        try:
            self.mc.set("this\x10has\x11control characters\x02", 1)
        except Client.MemcachedKeyCharacterError as err:
            self.assertTrue("characters not allowed" in err.args[0])
        else:
            self.fail(
                "Expected Client.MemcachedKeyCharacterError, nothing raised")

    def test_sending_key_too_long(self):
        try:
            self.mc.set('a' * SERVER_MAX_KEY_LENGTH + 'a', 1)
        except Client.MemcachedKeyLengthError as err:
            self.assertTrue("length is >" in err.args[0])
        else:
            self.fail(
                "Expected Client.MemcachedKeyLengthError, nothing raised")

        # These should work.
        self.mc.set('a' * SERVER_MAX_KEY_LENGTH, 1)
        self.mc.set('a' * SERVER_MAX_KEY_LENGTH, 1, noreply=True)

    def test_setget_boolean(self):
        """GitHub issue #75. Set/get with boolean values."""
        self.check_setget("bool", True)

    def test_unicode_key(self):
        s = u'\u4f1a'
        maxlen = SERVER_MAX_KEY_LENGTH // len(s.encode('utf-8'))
        key = s * maxlen

        self.mc.set(key, 5)
        value = self.mc.get(key)
        self.assertEqual(value, 5)

    def test_unicode_value(self):
        key = 'key'
        value = u'Iñtërnâtiônàlizætiøn2'
        self.mc.set(key, value)
        cached_value = self.mc.get(key)
        self.assertEqual(value, cached_value)

    def test_binary_string(self):
        value = 'value_to_be_compressed'
        compressed_value = zlib.compress(value.encode())

        self.mc.set('binary1', compressed_value)
        compressed_result = self.mc.get('binary1')
        self.assertEqual(compressed_value, compressed_result)
        self.assertEqual(value, zlib.decompress(compressed_result).decode())

        self.mc.add('binary1-add', compressed_value)
        compressed_result = self.mc.get('binary1-add')
        self.assertEqual(compressed_value, compressed_result)
        self.assertEqual(value, zlib.decompress(compressed_result).decode())

        self.mc.set_multi({'binary1-set_many': compressed_value})
        compressed_result = self.mc.get('binary1-set_many')
        self.assertEqual(compressed_value, compressed_result)
        self.assertEqual(value, zlib.decompress(compressed_result).decode())

    def test_ignore_too_large_value(self):
        # NOTE: "MemCached: while expecting[...]" is normal...
        key = 'keyhere'

        value = 'a' * (SERVER_MAX_VALUE_LENGTH // 2)
        self.assertTrue(self.mc.set(key, value))
        self.assertEqual(self.mc.get(key), value)

        value = 'a' * SERVER_MAX_VALUE_LENGTH
        with captured_stderr() as log:
            self.assertIs(self.mc.set(key, value), False)
        self.assertEqual(
            log.getvalue(),
            "MemCached: while expecting 'STORED', got unexpected response "
            "'SERVER_ERROR object too large for cache'\n"
        )
        # This test fails if the -I option is used on the memcached server
        self.assertTrue(self.mc.get(key) is None)

    def test_get_set_multi_key_prefix(self):
        """Testing set_multi() with no memcacheds running."""

        prefix = 'pfx_'
        values = {'key1': 'a', 'key2': 'b'}
        errors = self.mc.set_multi(values, key_prefix=prefix)
        self.assertEqual(errors, [])

        keys = list(values)
        self.assertEqual(self.mc.get_multi(keys, key_prefix=prefix),
                         values)

    def test_set_multi_dead_servers(self):
        """Testing set_multi() with no memcacheds running."""

        self.mc.disconnect_all()
        with captured_stderr() as log:
            for server in self.mc.servers:
                server.mark_dead('test')
        self.assertIn('Marking dead.', log.getvalue())
        errors = self.mc.set_multi({'key1': 'a', 'key2': 'b'})
        self.assertEqual(sorted(errors), ['key1', 'key2'])

    def test_disconnect_all_delete_multi(self):
        """Testing delete_multi() with no memcacheds running."""
        self.mc.disconnect_all()
        with captured_stderr() as output:
            ret = self.mc.delete_multi(('keyhere', 'keythere'))
        self.assertEqual(ret, 1)
        self.assertEqual(
            output.getvalue(),
            "MemCached: while expecting 'DELETED', got unexpected response "
            "'NOT_FOUND'\n"
            "MemCached: while expecting 'DELETED', got unexpected response "
            "'NOT_FOUND'\n"
        )

    @mock.patch.object(_Host, 'send_cmd')  # Don't send any commands.
    @mock.patch.object(_Host, 'readline')
    def test_touch_unexpected_reply(self, mock_readline, mock_send_cmd):
        """touch() logs an error upon receiving an unexpected reply."""
        mock_readline.return_value = 'SET'  # the unexpected reply
        with captured_stderr() as output:
            self.mc.touch('key')
        self.assertEqual(
            output.getvalue(),
            "MemCached: touch expected %s, got: 'SET'\n" % b'TOUCHED'
        )


if __name__ == '__main__':
    unittest.main()
