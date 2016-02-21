from __future__ import (
    print_function,
    absolute_import,
)

import os
import socket
import unittest

import mock
import six

from memcache import (
    const,
    exc,
    utils,
)
from memcache import Client


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
        # TODO(Jeremy) unix socket server stuff
        self.connections = [os.getenv("MEMCACHED") or "127.0.0.1:11211"]
        self.mc = Client(self.connections, debug=1)

    def tearDown(self):
        del self.mc

    def check_setget(self, key, val, noreply=False):
        self.mc.set(key, val, noreply=noreply)
        newval = self.mc.get(key)
        self.assertEqual(newval, val)

    def test_flush_all(self):
        self.mc.set('k', 'v')
        self.mc.flush_all()
        self.assertIsNone(self.mc.get('k'))

    def test_get_stats(self):
        self.assertNotEqual(self.mc.get_stats(), [])

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
        except exc.MemcachedKeyCharacterError as err:
            self.assertTrue("characters not allowed" in err.args[0])
        else:
            self.fail(
                "Expected exc.MemcachedKeyCharacterError, nothing raised")

    def test_sending_control_characters(self):
        try:
            self.mc.set("this\x10has\x11control characters\x02", 1)
        except exc.MemcachedKeyCharacterError as err:
            self.assertTrue("characters not allowed" in err.args[0])
        else:
            self.fail(
                "Expected exc.MemcachedKeyCharacterError, nothing raised")

    def test_sending_key_too_long(self):
        try:
            self.mc.set('a' * const.MAX_KEY_LENGTH + 'a', 1)
        except exc.MemcachedKeyLengthError as err:
            self.assertTrue("length is >" in err.args[0])
        else:
            self.fail(
                "Expected exc.MemcachedKeyLengthError, nothing raised")

        # These should work.
        self.mc.set('a' * const.MAX_KEY_LENGTH, 1)
        self.mc.set('a' * const.MAX_KEY_LENGTH, 1, noreply=True)

    def test_setget_boolean(self):
        """GitHub issue #75. Set/get with boolean values."""
        self.check_setget("bool", True)

    def test_unicode_key(self):
        s = six.u('\u4f1a')
        maxlen = const.MAX_KEY_LENGTH // len(s.encode('utf-8'))
        key = s * maxlen

        self.mc.set(key, 5)
        value = self.mc.get(key)
        self.assertEqual(value, 5)

    def test_ignore_too_large_value(self):
        # NOTE: "MemCached: while expecting[...]" is normal...
        key = 'keyhere'

        value = 'a' * (const.MAX_VALUE_LENGTH // 2)
        self.assertTrue(self.mc.set(key, value))
        self.assertEqual(self.mc.get(key), value)

        value = 'a' * const.MAX_VALUE_LENGTH
        self.assertFalse(self.mc.set(key, value))
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
        for conn in self.mc.connections:
            conn.mark_dead('test')
        errors = self.mc.set_multi({'key1': 'a', 'key2': 'b'})
        self.assertEqual(sorted(errors), ['key1', 'key2'])

    def test_disconnect_all_delete_multi(self):
        """Testing delete_multi() with no memcacheds running."""
        ret = self.mc.delete_multi({'keyhere': 'a', 'keythere': 'b'})
        self.assertEqual(ret, 1)

    def test_delete_multi_doctest(self):
        self.mc.set_multi({'a1': 'val1', 'a2': 'val2'})
        self.assertEqual(self.mc.get_multi(['a1', 'a2']),
                         {'a1': 'val1', 'a2': 'val2'})
        self.assertTrue(self.mc.delete_multi(['key1', 'key2']))
        self.assertEqual(self.mc.get_multi(['key1', 'key2']), {})

    def test_delete_multi_noreply(self):
        self.mc.set_multi({'a1': 'val1', 'a2': 'val2'})
        self.assertTrue(self.mc.delete_multi(['key1', 'key2'], noreply=True))

    def test_delete_multi_dead_connections(self):
        mc = Client(self.connections * 2, debug=1)
        mc.set_multi({'a1': 'val1', 'a2': 'val2'})
        conn = next(iter(mc.connections))
        with mock.patch.object(conn, 'send', side_effect=socket.error):
            self.assertFalse(mc.delete_multi(['key1', 'key2']))

    def test_delete_multi_dead_connections_fail_read(self):
        mc = Client(self.connections * 2, debug=1)
        mc.set_multi({'a1': 'val1', 'a2': 'val2'})
        conn = next(iter(mc.connections))
        with mock.patch.object(conn, 'expect', side_effect=socket.error):
            self.assertFalse(mc.delete_multi(['key1', 'key2']))

    def test_add(self):
        self.mc.set('k', 'v')
        self.assertFalse(self.mc.add('k', 'v'))

    def test_add_no_server(self):
        r = (None, None)
        with mock.patch.object(self.mc.connections, 'get', return_value=r):
            self.assertEqual(self.mc.add('k', 'v'), 0)

    def test_incr_doctest(self):
        self.mc.set("counter", "20")
        self.assertEqual(self.mc.incr("counter"), 21)
        self.assertEqual(self.mc.incr("counter"), 22)

    def test_set_multi_doctest(self):
        self.mc.set_multi({'key1': 'val1', 'key2': 'val2'})
        self.assertEqual(self.mc.get_multi(['key1', 'key2']),
                         {'key1': 'val1', 'key2': 'val2'})

    def test_set_multi_notset_doctest(self):
        notset_keys = self.mc.set_multi({'key1': 'val1', 'key2': 'val2'},
                                        key_prefix='subspace_')
        self.assertFalse(notset_keys)

    def test_get_multi_doctest(self):
        self.mc.set("foo", "bar")
        self.mc.set("baz", 42)
        self.assertEqual(self.mc.get_multi(["foo", "baz", "foobar"]),
                         {"foo": "bar", "baz": 42})
        self.assertEqual(self.mc.set_multi({'k1': 1, 'k2': 2},
                                           key_prefix='pfx_'), [])

    def test_get_multi_numbers_doctest(self):
        self.mc.set_multi({42: 'douglass adams',
                          46: 'and 2 just ahead of me'},
                          key_prefix='numkeys_')
        self.assertEqual(self.mc.get_multi([46, 42], key_prefix='numkeys_'),
                         {42: 'douglass adams', 46: 'and 2 just ahead of me'})

    def test_encode_key(self):
        self.assertEqual(
            utils.encode_key(('a', u'b')),
            ('a', b'b'))


if __name__ == '__main__':
    unittest.main()
