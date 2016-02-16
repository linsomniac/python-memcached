#!/usr/bin/env python
"""client module for memcached (memory cache daemon)

Overview
========

See U{the MemCached homepage<http://www.danga.com/memcached>} for more
about memcached.

Usage summary
=============

This should give you a feel for how this module operates::

    import memcache
    mc = memcache.Client(['127.0.0.1:11211'], debug=0)

    mc.set("some_key", "Some value")
    value = mc.get("some_key")

    mc.set("another_key", 3)
    mc.delete("another_key")

    mc.set("key", "1") # note that the key used for incr/decr must be
                       # a string.
    mc.incr("key")
    mc.decr("key")

The standard way to use memcache with a database is like this:

    key = derive_key(obj)
    obj = mc.get(key)
    if not obj:
        obj = backend_api.get(...)
        mc.set(key, obj)

    # we now have obj, and future passes through this code
    # will use the object from the cache.

Detailed Documentation
======================

More detailed documentation is available in the L{Client} class.

"""
from __future__ import (
    print_function,
    absolute_import,
)

__all__ = [
    'Client',
    'exc',
    'SERVER_MAX_KEY_LENGTH',
    'SERVER_MAX_VALUE_LENGTH',
]
#  Original author: Evan Martin of Danga Interactive
__author__ = "Sean Reifschneider <jafo-memcached@tummy.com>"
__version__ = "1.57"
__copyright__ = "Copyright (C) 2003 Danga Interactive"
#  http://en.wikipedia.org/wiki/Python_Software_Foundation_License
__license__ = "Python Software Foundation License"


from . import exc
from .client import (
    Client,
    SERVER_MAX_KEY_LENGTH,
    SERVER_MAX_VALUE_LENGTH,
)
