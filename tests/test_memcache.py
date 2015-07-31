from __future__ import print_function

from unittest import TestCase

import six

from memcache import Client, SERVER_MAX_KEY_LENGTH

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


class TestMemcache(TestCase):
    def setUp(self):
        # TODO: unix socket server stuff
        servers = ["127.0.0.1:11211"]
        self.mc = Client(servers, debug=1)
        pass

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

if __name__ == "__main__":
    # failures = 0
    # print("Testing docstrings...")
    # _doctest()
    # print("Running tests:")
    # print()
    # serverList = [["127.0.0.1:11211"]]
    # if '--do-unix' in sys.argv:
    #     serverList.append([os.path.join(os.getcwd(), 'memcached.socket')])

    # for servers in serverList:
    #     mc = Client(servers, debug=1)
    if False:

        print("Testing sending a unicode-string key...", end=" ")
        try:
            x = mc.set(six.u('keyhere'), 1)
        except Client.MemcachedStringEncodingError as msg:
            print("OK", end=" ")
        else:
            print("FAIL", end=" ")
            failures += 1
        try:
            x = mc.set((six.u('a')*SERVER_MAX_KEY_LENGTH).encode('utf-8'), 1)
        except Client.MemcachedKeyError:
            print("FAIL", end=" ")
            failures += 1
        else:
            print("OK", end=" ")
        s = pickle.loads('V\\u4f1a\np0\n.')
        try:
            x = mc.set((s * SERVER_MAX_KEY_LENGTH).encode('utf-8'), 1)
        except Client.MemcachedKeyLengthError:
            print("OK")
        else:
            print("FAIL")
            failures += 1

        print("Testing using a value larger than the memcached value limit...")
        print('NOTE: "MemCached: while expecting[...]" is normal...')
        x = mc.set('keyhere', 'a'*SERVER_MAX_VALUE_LENGTH)
        if mc.get('keyhere') is None:
            print("OK", end=" ")
        else:
            print("FAIL", end=" ")
            failures += 1
        x = mc.set('keyhere', 'a'*SERVER_MAX_VALUE_LENGTH + 'aaa')
        if mc.get('keyhere') is None:
            print("OK")
        else:
            print("FAIL")
            failures += 1

        print("Testing set_multi() with no memcacheds running", end=" ")
        mc.disconnect_all()
        errors = mc.set_multi({'keyhere': 'a', 'keythere': 'b'})
        if errors != []:
            print("FAIL")
            failures += 1
        else:
            print("OK")

        print("Testing delete_multi() with no memcacheds running", end=" ")
        mc.disconnect_all()
        ret = mc.delete_multi({'keyhere': 'a', 'keythere': 'b'})
        if ret != 1:
            print("FAIL")
            failures += 1
        else:
            print("OK")

    if failures > 0:
        print('*** THERE WERE FAILED TESTS')
        sys.exit(1)
    sys.exit(0)
