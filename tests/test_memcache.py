from __future__ import print_function

import unittest

import six

from memcache import Client, SERVER_MAX_KEY_LENGTH, SERVER_MAX_VALUE_LENGTH

try:
    _str_cls = basestring
except NameError:
    _str_cls = str


def to_s(val):
    if not isinstance(val, _str_cls):
        return "%s (%s)" % (val, type(val))
    return "%s" % val


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
        # TODO: unix socket server stuff
        servers = ["127.0.0.1:11211"]
        self.mc = Client(servers, debug=1)

    def tearDown(self):
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
        s = six.u('\u4f1a')
        maxlen = SERVER_MAX_KEY_LENGTH // len(s.encode('utf-8'))
        key = s * maxlen

        self.mc.set(key, 5)
        value = self.mc.get(key)
        self.assertEqual(value, 5)

    def test_ignore_too_large_value(self):
        # NOTE: "MemCached: while expecting[...]" is normal...
        key = 'keyhere'

        value = 'a' * (SERVER_MAX_VALUE_LENGTH // 2)
        self.assertTrue(self.mc.set(key, value))
        self.assertEqual(self.mc.get(key), value)

        value = 'a' * SERVER_MAX_VALUE_LENGTH
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

        self.mc.disconnect_all()
        for server in self.mc.servers:
            server.mark_dead('test')
        errors = self.mc.set_multi({'key1': 'a', 'key2': 'b'})
        self.assertEqual(sorted(errors), ['key1', 'key2'])

    def test_disconnect_all_delete_multi(self):
        """Testing delete_multi() with no memcacheds running."""
        self.mc.disconnect_all()
        ret = self.mc.delete_multi({'keyhere': 'a', 'keythere': 'b'})
        self.assertEqual(ret, 1)


if __name__ == '__main__':
    unittest.main()
