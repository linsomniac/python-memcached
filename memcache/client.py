from __future__ import (
    print_function,
    absolute_import,
)

import logging
import re
import socket
import threading

import six

from . import (
    connection,
    const,
    exc,
    utils,

)


class Client(threading.local):
    """Object representing a pool of memcache servers.

    See L{memcache} for an overview.

    In all cases where a key is used, the key can be either:
        1. A simple hashable type (string, integer, etc.).
        2. A tuple of C{(hashvalue, key)}.  This is useful if you want
        to avoid making this module calculate a hash value.  You may
        prefer, for example, to keep all of a given user's objects on
        the same memcache server, so you could use the user's unique
        id as the hash value.


    @group Setup: __init__, set_servers, forget_dead_hosts,
    disconnect_all, debuglog
    @group Insertion: set, add, replace, set_multi
    @group Retrieval: get, get_multi
    @group Integers: incr, decr
    @group Removal: delete, delete_multi
    @sort: __init__, set_servers, forget_dead_hosts, disconnect_all,
           debuglog,\ set, set_multi, add, replace, get, get_multi,
           incr, decr, delete, delete_multi
    """
    CONNECTIONS = connection.ConnectionPool

    def __init__(self, servers, debug=False,
                 pload=None, pid=None,
                 dead_retry=None, socket_timeout=None,
                 cache_cas=False, flush_on_reconnect=None):
        """Create a new Client object with the given list of servers.

        @param servers: C{servers} is passed to L{set_servers}.
        @param debug: whether to display error messages when a server
        can't be contacted.
        @param pickleProtocol: number to mandate protocol used by
        (c)Pickle.
        @param pickler: optional override of default Pickler to allow
        subclassing.
        @param unpickler: optional override of default Unpickler to
        allow subclassing.
        @param pload: optional persistent_load function to call on
        pickle loading.  Useful for cPickle since subclassing isn't
        allowed.
        @param pid: optional persistent_id function to call on pickle
        storing.  Useful for cPickle since subclassing isn't allowed.
        @param dead_retry: number of seconds before retrying a
        blacklisted server. Default to 30 s.
        @param socket_timeout: timeout in seconds for all calls to a
        server. Defaults to 3 seconds.
        @param cache_cas: (default False) If true, cas operations will
        be cached.  WARNING: This cache is not expired internally, if
        you have a long-running process you will need to expire it
        manually via client.reset_cas(), or the cache can grow
        unlimited.
        @param server_max_key_length: (default MAX_KEY_LENGTH)
        Data that is larger than this will not be sent to the server.
        @param server_max_value_length: (default
        SERVER_MAX_VALUE_LENGTH) Data that is larger than this will
        not be sent to the server.
        @param flush_on_reconnect: optional flag which prevents a
        scenario that can cause stale data to be read: If there's more
        than one memcached server and the connection to one is
        interrupted, keys that mapped to that server will get
        reassigned to another. If the first server comes back, those
        keys will map to it again. If it still has its data, get()s
        can read stale data that was overwritten on another
        server. This flag is off by default for backwards
        compatibility.
        """
        super(Client, self).__init__()
        self.debug = debug
        self.stats = {}
        self.cache_cas = cache_cas
        self.reset_cas()

        self.persistent_load = pload
        self.persistent_id = pid

        self.logger = logging.getLogger('memcache.client')

        conn_settings = {
            'persistent_id': pid,
            'persistent_load': pload,
            'dead_retry': dead_retry,
            'socket_timeout': socket_timeout,
            'flush_on_reconnect': flush_on_reconnect,
        }
        self.connections = self.CONNECTIONS(
            servers,
            conn_settings,
        )

    def reset_cas(self):
        """Reset the cas cache.

        This is only used if the Client() object was created with
        "cache_cas=True".  If used, this cache does not expire
        internally, so it can grow unbounded if you do not clear it
        yourself.
        """
        self.cas_ids = {}

    def get_stats(self, stat_args=None):
        """Get statistics from each of the servers.

        @param stat_args: Additional arguments to pass to the memcache
            "stats" command.

        @return: A list of tuples ( server_identifier,
            stats_dictionary ).  The dictionary contains a number of
            name/value pairs specifying the name of the status field
            and the string value associated with it.  The values are
            not converted from strings.
        """
        data = []
        for s in self.connections:
            if not s.connect():
                continue
            if s.family == socket.AF_INET:
                name = '%s:%s (%s)' % (s.ip, s.port, s.weight)
            elif s.family == socket.AF_INET6:
                name = '[%s]:%s (%s)' % (s.ip, s.port, s.weight)
            else:
                name = 'unix:%s (%s)' % (s.address, s.weight)
            if not stat_args:
                s.send_one('stats')
            else:
                s.send_one('stats ' + stat_args)
            serverData = {}
            data.append((name, serverData))
            readline = s.readline
            while 1:
                line = readline()
                if not line or line.strip() == 'END':
                    break
                stats = line.split(' ', 2)
                serverData[stats[1]] = stats[2]

        return(data)

    def flush_all(self):
        """Expire all data in memcache servers that are reachable."""
        for s in self.connections:
            if not s.connect():
                continue
            s.flush()

    def delete_multi(self, keys, time=0, key_prefix='', noreply=False):
        """Delete multiple keys in the memcache doing just one query.

        >>> notset_keys = mc.set_multi({'a1' : 'val1', 'a2' : 'val2'})
        >>> mc.get_multi(['a1', 'a2']) == {'a1' : 'val1','a2' : 'val2'}
        1
        >>> mc.delete_multi(['key1', 'key2'])
        1
        >>> mc.get_multi(['key1', 'key2']) == {}
        1

        This method is recommended over iterated regular L{delete}s as
        it reduces total latency, since your app doesn't have to wait
        for each round-trip of L{delete} before sending the next one.

        @param keys: An iterable of keys to clear
        @param time: number of seconds any subsequent set / update
        commands should fail. Defaults to 0 for no delay.
        @param key_prefix: Optional string to prepend to each key when
            sending to memcache.  See docs for L{get_multi} and
            L{set_multi}.
        @param noreply: optional parameter instructs the server to not send the
            reply.
        @return: 1 if no failure in communication with any memcacheds.
        @rtype: int
        """

        server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(
            keys, key_prefix)

        # send out all requests on each server before reading anything
        dead_servers = []

        rc = 1
        for server in six.iterkeys(server_keys):
            bigcmd = []
            write = bigcmd.append
            if time is not None:
                headers = str(time)
            else:
                headers = None
            for key in server_keys[server]:  # These are mangled keys
                cmd = utils.encode_command('delete', key, headers,
                                           noreply, b'\r\n')
                write(cmd)
            try:
                server.send(bigcmd)
            except socket.error as msg:
                rc = 0
                if isinstance(msg, tuple):
                    msg = msg[1]
                server.mark_dead(msg)
                dead_servers.append(server)

        # if noreply, just return
        if noreply:
            return rc

        # if any servers died on the way, don't expect them to respond.
        for server in dead_servers:
            del server_keys[server]

        for server, keys in six.iteritems(server_keys):
            try:
                for key in keys:
                    server.expect(b"DELETED")
            except socket.error as msg:
                if isinstance(msg, tuple):
                    msg = msg[1]
                server.mark_dead(msg)
                rc = 0
        return rc

    def delete(self, key, time=0, noreply=False):
        '''Deletes a key from the memcache.

        @return: Nonzero on success.
        @param time: number of seconds any subsequent set / update commands
        should fail. Defaults to None for no delay.
        @param noreply: optional parameter instructs the server to not send the
            reply.
        @rtype: int
        '''
        return self._deletetouch([b'DELETED', b'NOT_FOUND'], "delete", key,
                                 time, noreply)

    def touch(self, key, time=0, noreply=False):
        '''Updates the expiration time of a key in memcache.

        @return: Nonzero on success.
        @param time: Tells memcached the time which this value should
            expire, either as a delta number of seconds, or an absolute
            unix time-since-the-epoch value. See the memcached protocol
            docs section "Storage Commands" for more info on <exptime>. We
            default to 0 == cache forever.
        @param noreply: optional parameter instructs the server to not send the
            reply.
        @rtype: int
        '''
        return self._deletetouch([b'TOUCHED'], "touch", key, time, noreply)

    def _deletetouch(self, expected, cmd, key, time=0, noreply=False):
        key = utils.encode_key(key)
        self.check_key(key)
        server, key = self.connections.get(key)
        if not server:
            return 0
        if time is not None and time != 0:
            headers = str(time)
        else:
            headers = None
        fullcmd = utils.encode_command(cmd, key, headers, noreply)

        try:
            server.send_one(fullcmd)
            if noreply:
                return 1
            line = server.readline()
            if line and line.strip() in expected:
                return 1
            self.logger.debug('%s expected %s, got: %r'
                              % (cmd, ' or '.join(expected), line))
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            server.mark_dead(msg)
        return 0

    def incr(self, key, delta=1, noreply=False):
        """Increment value for C{key} by C{delta}

        Sends a command to the server to atomically increment the
        value for C{key} by C{delta}, or by 1 if C{delta} is
        unspecified.  Returns None if C{key} doesn't exist on server,
        otherwise it returns the new value after incrementing.

        Note that the value for C{key} must already exist in the
        memcache, and it must be the string representation of an
        integer.

        >>> mc.set("counter", "20")  # returns 1, indicating success
        1
        >>> mc.incr("counter")
        21
        >>> mc.incr("counter")
        22

        Overflow on server is not checked.  Be aware of values
        approaching 2**32.  See L{decr}.

        @param delta: Integer amount to increment by (should be zero
        or greater).

        @param noreply: optional parameter instructs the server to not send the
        reply.

        @return: New value after incrementing, no None for noreply or error.
        @rtype: int
        """
        return self._incrdecr("incr", key, delta, noreply)

    def decr(self, key, delta=1, noreply=False):
        """Decrement value for C{key} by C{delta}

        Like L{incr}, but decrements.  Unlike L{incr}, underflow is
        checked and new values are capped at 0.  If server value is 1,
        a decrement of 2 returns 0, not -1.

        @param delta: Integer amount to decrement by (should be zero
        or greater).

        @param noreply: optional parameter instructs the server to not send the
        reply.

        @return: New value after decrementing,  or None for noreply or error.
        @rtype: int
        """
        return self._incrdecr("decr", key, delta, noreply)

    def _incrdecr(self, cmd, key, delta, noreply=False):
        key = utils.encode_key(key)
        self.check_key(key)
        server, key = self.connections.get(key)
        if not server:
            return None
        fullcmd = utils.encode_command(cmd, key, str(delta), noreply)
        try:
            server.send_one(fullcmd)
            if noreply:
                return
            line = server.readline()
            if line is None or line.strip() == b'NOT_FOUND':
                return None
            return int(line)
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            server.mark_dead(msg)
            return None

    def add(self, key, val, time=0, min_compress_len=0, noreply=False):
        '''Add new key with value.

        Like L{set}, but only stores in memcache if the key doesn't
        already exist.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("add", key, val, time, min_compress_len, noreply)

    def append(self, key, val, time=0, min_compress_len=0, noreply=False):
        '''Append the value to the end of the existing key's value.

        Only stores in memcache if key already exists.
        Also see L{prepend}.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("append", key, val, time, min_compress_len, noreply)

    def prepend(self, key, val, time=0, min_compress_len=0, noreply=False):
        '''Prepend the value to the beginning of the existing key's value.

        Only stores in memcache if key already exists.
        Also see L{append}.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("prepend", key, val, time, min_compress_len, noreply)

    def replace(self, key, val, time=0, min_compress_len=0, noreply=False):
        '''Replace existing key with value.

        Like L{set}, but only stores in memcache if the key already exists.
        The opposite of L{add}.

        @return: Nonzero on success.
        @rtype: int
        '''
        return self._set("replace", key, val, time, min_compress_len, noreply)

    def set(self, key, val, time=0, min_compress_len=0, noreply=False):
        '''Unconditionally sets a key to a given value in the memcache.

        The C{key} can optionally be an tuple, with the first element
        being the server hash value and the second being the key.  If
        you want to avoid making this module calculate a hash value.
        You may prefer, for example, to keep all of a given user's
        objects on the same memcache server, so you could use the
        user's unique id as the hash value.

        @return: Nonzero on success.
        @rtype: int

        @param time: Tells memcached the time which this value should
        expire, either as a delta number of seconds, or an absolute
        unix time-since-the-epoch value. See the memcached protocol
        docs section "Storage Commands" for more info on <exptime>. We
        default to 0 == cache forever.

        @param min_compress_len: The threshold length to kick in
        auto-compression of the value using the compressor
        routine. If the value being cached is a string, then the
        length of the string is measured, else if the value is an
        object, then the length of the pickle result is measured. If
        the resulting attempt at compression yeilds a larger string
        than the input, then it is discarded. For backwards
        compatability, this parameter defaults to 0, indicating don't
        ever try to compress.

        @param noreply: optional parameter instructs the server to not
        send the reply.
        '''
        return self._set("set", key, val, time, min_compress_len, noreply)

    def cas(self, key, val, time=0, min_compress_len=0, noreply=False):
        '''Check and set (CAS)

        Sets a key to a given value in the memcache if it hasn't been
        altered since last fetched. (See L{gets}).

        The C{key} can optionally be an tuple, with the first element
        being the server hash value and the second being the key.  If
        you want to avoid making this module calculate a hash value.
        You may prefer, for example, to keep all of a given user's
        objects on the same memcache server, so you could use the
        user's unique id as the hash value.

        @return: Nonzero on success.
        @rtype: int

        @param time: Tells memcached the time which this value should
        expire, either as a delta number of seconds, or an absolute
        unix time-since-the-epoch value. See the memcached protocol
        docs section "Storage Commands" for more info on <exptime>. We
        default to 0 == cache forever.

        @param min_compress_len: The threshold length to kick in
        auto-compression of the value using the compressor
        routine. If the value being cached is a string, then the
        length of the string is measured, else if the value is an
        object, then the length of the pickle result is measured. If
        the resulting attempt at compression yeilds a larger string
        than the input, then it is discarded. For backwards
        compatability, this parameter defaults to 0, indicating don't
        ever try to compress.

        @param noreply: optional parameter instructs the server to not
        send the reply.
        '''
        return self._set("cas", key, val, time, min_compress_len, noreply)

    def _map_and_prefix_keys(self, key_iterable, key_prefix):
        '''Compute the mapping of server.

        (connection.Connection instance) -> list of keys to stuff onto
        that server, as well as the mapping of prefixed key -> original key.
        '''
        key_prefix = utils.encode_key(key_prefix)
        # Check it just once ...
        key_extra_len = len(key_prefix)
        if key_prefix:
            self.check_key(key_prefix)

        # server (connection.Connection) ->
        # list of unprefixed server keys in mapping
        server_keys = {}

        prefixed_to_orig_key = {}
        # build up a list for each server of all the keys we want.
        for orig_key in key_iterable:
            if isinstance(orig_key, tuple):
                # Tuple of hashvalue, key ala _get_server(). Caller is
                # essentially telling us what server to stuff this on.
                # Ensure call to _get_server gets a Tuple as well.
                serverhash, key = orig_key

                key = utils.encode_key(key)
                if not isinstance(key, six.binary_type):
                    # set_multi supports int / long keys.
                    key = str(key)
                    if six.PY3:
                        key = key.encode('utf8')
                bytes_orig_key = key

                # Gotta pre-mangle key before hashing to a
                # server. Returns the mangled key.
                server, key = self.connections.get(
                    (serverhash, key_prefix + key))

                orig_key = orig_key[1]
            else:
                key = utils.encode_key(orig_key)
                if not isinstance(key, six.binary_type):
                    # set_multi supports int / long keys.
                    key = str(key)
                    if six.PY3:
                        key = key.encode('utf8')
                bytes_orig_key = key
                server, key = self.connections.get(key_prefix + key)

            #  alert when passed in key is None
            if orig_key is None:
                self.check_key(orig_key, key_extra_len=key_extra_len)

            # Now check to make sure key length is proper ...
            self.check_key(bytes_orig_key, key_extra_len=key_extra_len)

            if not server:
                continue

            if server not in server_keys:
                server_keys[server] = []
            server_keys[server].append(key)
            prefixed_to_orig_key[key] = orig_key

        return (server_keys, prefixed_to_orig_key)

    def set_multi(self, mapping, time=0, key_prefix='', min_compress_len=0,
                  noreply=False):
        '''Sets multiple keys in the memcache doing just one query.

        >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'})
        >>> mc.get_multi(['key1', 'key2']) == {'key1' : 'val1',
        ...                                    'key2' : 'val2'}
        1


        This method is recommended over regular L{set} as it lowers
        the number of total packets flying around your network,
        reducing total latency, since your app doesn't have to wait
        for each round-trip of L{set} before sending the next one.

        @param mapping: A dict of key/value pairs to set.

        @param time: Tells memcached the time which this value should
            expire, either as a delta number of seconds, or an
            absolute unix time-since-the-epoch value. See the
            memcached protocol docs section "Storage Commands" for
            more info on <exptime>. We default to 0 == cache forever.

        @param key_prefix: Optional string to prepend to each key when
            sending to memcache. Allows you to efficiently stuff these
            keys into a pseudo-namespace in memcache:

            >>> notset_keys = mc.set_multi(
            ...     {'key1' : 'val1', 'key2' : 'val2'},
            ...     key_prefix='subspace_')
            >>> len(notset_keys) == 0
            True
            >>> mc.get_multi(['subspace_key1',
            ...               'subspace_key2']) == {'subspace_key1': 'val1',
            ...                                     'subspace_key2' : 'val2'}
            True

            Causes key 'subspace_key1' and 'subspace_key2' to be
            set. Useful in conjunction with a higher-level layer which
            applies namespaces to data in memcache.  In this case, the
            return result would be the list of notset original keys,
            prefix not applied.

        @param min_compress_len: The threshold length to kick in
            auto-compression of the value using the compressor
            routine. If the value being cached is a string, then the
            length of the string is measured, else if the value is an
            object, then the length of the pickle result is
            measured. If the resulting attempt at compression yeilds a
            larger string than the input, then it is discarded. For
            backwards compatability, this parameter defaults to 0,
            indicating don't ever try to compress.

        @param noreply: optional parameter instructs the server to not
            send the reply.

        @return: List of keys which failed to be stored [ memcache out
           of memory, etc. ].

        @rtype: list
        '''
        server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(
            six.iterkeys(mapping), key_prefix)

        # send out all requests on each server before reading anything
        dead_servers = []
        notstored = []  # original keys.

        for server in six.iterkeys(server_keys):
            bigcmd = []
            write = bigcmd.append
            try:
                for key in server_keys[server]:  # These are mangled keys
                    store_info = server.convert_value(
                        mapping[prefixed_to_orig_key[key]],
                        min_compress_len)
                    if store_info:
                        flags, len_val, val = store_info
                        headers = "%d %d %d" % (flags, time, len_val)
                        fullcmd = utils.encode_command('set', key, headers,
                                                       noreply,
                                                       b'\r\n', val, b'\r\n')
                        write(fullcmd)
                    else:
                        notstored.append(prefixed_to_orig_key[key])
                server.send(bigcmd)
            except socket.error as msg:
                if isinstance(msg, tuple):
                    msg = msg[1]
                server.mark_dead(msg)
                dead_servers.append(server)

        # if noreply, just return early
        if noreply:
            return notstored

        # if any servers died on the way, don't expect them to respond.
        for server in dead_servers:
            del server_keys[server]

        #  short-circuit if there are no servers, just return all keys
        if not server_keys:
            return list(mapping.keys())

        for server, keys in six.iteritems(server_keys):
            try:
                for key in keys:
                    if server.readline() == b'STORED':
                        continue
                    else:
                        # un-mangle.
                        notstored.append(prefixed_to_orig_key[key])
            except (exc.MemcachedError, socket.error) as msg:
                if isinstance(msg, tuple):
                    msg = msg[1]
                server.mark_dead(msg)
        return notstored

    def _unsafe_set(self, cmd, key, val, time, min_compress_len,
                    noreply, server):
        if cmd == 'cas' and key not in self.cas_ids:
            return self._set('set', key, val, time, min_compress_len,
                             noreply)

        store_info = server.convert_value(val, min_compress_len)
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
            server.send_one(fullcmd)
            if noreply:
                return True
            return(server.expect(b"STORED", raise_exception=True)
                   == b"STORED")
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            server.mark_dead(msg)
        return 0

    def _set(self, cmd, key, val, time, min_compress_len=0, noreply=False):
        key = utils.encode_key(key)
        self.check_key(key)
        server, key = self.connections.get(key)
        if not server:
            return 0

        try:
            return self._unsafe_set(cmd, key, val, time, min_compress_len,
                                    noreply, server)
        except exc.MemcachedConnectionDeadError:
            # retry once
            try:
                if server.connect():
                    return self._unsafe_set(cmd, key, val, time,
                                            min_compress_len, noreply,
                                            server)
            except (exc.MemcachedConnectionDeadError, socket.error) as msg:
                server.mark_dead(msg)
            return 0

    def _unsafe_get(self, cmd, key, server):
        try:
            cmd_bytes = cmd.encode('utf-8') if six.PY3 else cmd
            fullcmd = b''.join((cmd_bytes, b' ', key))
            server.send_one(fullcmd)
            rkey = flags = rlen = cas_id = None

            if cmd == 'gets':
                rkey, flags, rlen, cas_id, = \
                    server.expect_cas_value(raise_exception=True)
                if rkey and self.cache_cas:
                    self.cas_ids[rkey] = cas_id
            else:
                rkey, flags, rlen, = \
                    server.expect_value(raise_exception=True)

            if not rkey:
                return None
            try:
                value = server.recv_value(flags, rlen)
            finally:
                server.expect(b"END", raise_exception=True)
        except (exc.MemcachedError, socket.error) as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            server.mark_dead(msg)
            return None

        return value

    def _get(self, cmd, key):
        key = utils.encode_key(key)
        self.check_key(key)
        server, key = self.connections.get(key)
        if not server:
            return None

        try:
            return self._unsafe_get(cmd, key, server)
        except exc.MemcachedConnectionDeadError:
            # retry once
            try:
                if server.connect():
                    return self._unsafe_get(cmd, key, server)
                return None
            except (exc.MemcachedConnectionDeadError, socket.error) as msg:
                server.mark_dead(msg)
            return None

    def get(self, key):
        '''Retrieves a key from the memcache.

        @return: The value or None.
        '''
        return self._get('get', key)

    def gets(self, key):
        '''Retrieves a key from the memcache. Used in conjunction with 'cas'.

        @return: The value or None.
        '''
        return self._get('gets', key)

    def get_multi(self, keys, key_prefix=''):
        '''Retrieves multiple keys from the memcache doing just one query.

        >>> success = mc.set("foo", "bar")
        >>> success = mc.set("baz", 42)
        >>> mc.get_multi(["foo", "baz", "foobar"]) == {
        ...     "foo": "bar", "baz": 42
        ... }
        1
        >>> mc.set_multi({'k1' : 1, 'k2' : 2}, key_prefix='pfx_') == []
        1

        This looks up keys 'pfx_k1', 'pfx_k2', ... . Returned dict
        will just have unprefixed keys 'k1', 'k2'.

        >>> mc.get_multi(['k1', 'k2', 'nonexist'],
        ...              key_prefix='pfx_') == {'k1' : 1, 'k2' : 2}
        1

        get_mult [ and L{set_multi} ] can take str()-ables like ints /
        longs as keys too. Such as your db pri key fields.  They're
        rotored through str() before being passed off to memcache,
        with or without the use of a key_prefix.  In this mode, the
        key_prefix could be a table name, and the key itself a db
        primary key number.

        >>> mc.set_multi({42: 'douglass adams',
        ...               46: 'and 2 just ahead of me'},
        ...              key_prefix='numkeys_') == []
        1
        >>> mc.get_multi([46, 42], key_prefix='numkeys_') == {
        ...     42: 'douglass adams',
        ...     46: 'and 2 just ahead of me'
        ... }
        1

        This method is recommended over regular L{get} as it lowers
        the number of total packets flying around your network,
        reducing total latency, since your app doesn't have to wait
        for each round-trip of L{get} before sending the next one.

        See also L{set_multi}.

        @param keys: An array of keys.

        @param key_prefix: A string to prefix each key when we
        communicate with memcache.  Facilitates pseudo-namespaces
        within memcache. Returned dictionary keys will not have this
        prefix.

        @return: A dictionary of key/value pairs that were
        available. If key_prefix was provided, the keys in the retured
        dictionary will not have it present.
        '''
        server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(
            keys, key_prefix)

        # send out all requests on each server before reading anything
        dead_servers = []
        for server in six.iterkeys(server_keys):
            try:
                fullcmd = b"get " + b" ".join(server_keys[server])
                server.send_one(fullcmd)
            except socket.error as msg:
                if isinstance(msg, tuple):
                    msg = msg[1]
                server.mark_dead(msg)
                dead_servers.append(server)

        # if any servers died on the way, don't expect them to respond.
        for server in dead_servers:
            del server_keys[server]

        retvals = {}
        for server in six.iterkeys(server_keys):
            try:
                line = server.readline()
                while line and line != b'END':
                    rkey, flags, rlen = server.expect_value(line)
                    #  Bo Yang reports that this can sometimes be None
                    if rkey is not None:
                        val = server.recv_value(flags, rlen)
                        # un-prefix returned key.
                        retvals[prefixed_to_orig_key[rkey]] = val
                    line = server.readline()
            except (exc.MemcachedError, socket.error) as msg:
                if isinstance(msg, tuple):
                    msg = msg[1]
                server.mark_dead(msg)
        return retvals

    def check_key(self, key, key_extra_len=0):
        """Checks sanity of key.

            Fails if:

            Key length is > MAX_KEY_LENGTH (Raises MemcachedKeyLength).
            Contains control characters  (Raises MemcachedKeyCharacterError).
            Is not a string (Raises MemcachedStringEncodingError)
            Is an unicode string (Raises MemcachedStringEncodingError)
            Is not a string (Raises exc.MemcachedKeyError)
            Is None (Raises exc.MemcachedKeyError)
        """
        if isinstance(key, tuple):
            key = key[1]
        if key is None:
            raise exc.MemcachedKeyNoneError("Key is None")
        if key is '':
            if key_extra_len is 0:
                raise exc.MemcachedKeyNoneError("Key is empty")

            #  key is empty but there is some other component to key
            return

        if not isinstance(key, six.binary_type):
            raise exc.MemcachedKeyTypeError("Key must be a binary string")

        if (const.MAX_KEY_LENGTH != 0 and
                len(key) + key_extra_len > const.MAX_KEY_LENGTH):
            raise exc.MemcachedKeyLengthError(
                "Key length is > %s" % const.MAX_KEY_LENGTH
            )
        if not const.REGEX_VALID_KEY.match(key):
            raise exc.MemcachedKeyCharacterError(
                "Control/space characters not allowed (key=%r)" % key)

    def __del__(self):
        for conn in self.connections:
            conn.close()

    def __repr__(self):
        return '<memcache.Client>'
