from __future__ import (
    print_function,
    absolute_import,
)

import six

from . import (
    const,
    exc,
)


def encode_command(cmd, key, headers, noreply, *args):
    cmd_bytes = cmd.encode('utf-8') if six.PY3 else cmd
    fullcmd = [cmd_bytes, b' ', key]

    if headers:
        if six.PY3:
            headers = headers.encode('utf-8')
        fullcmd.append(b' ')
        fullcmd.append(headers)

    if noreply:
        fullcmd.append(b' noreply')

    if args:
        fullcmd.append(b' ')
        fullcmd.extend(args)
    return b''.join(fullcmd)


def encode_key(key):
    if isinstance(key, tuple):
        if isinstance(key[1], six.text_type):
            return (key[0], key[1].encode('utf8'))
    elif isinstance(key, six.text_type):
        return key.encode('utf8')
    return key


def check_key(key, key_extra_len=0):
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

    if not isinstance(key, six.binary_type):
        raise exc.MemcachedKeyTypeError("Key must be a binary string")

    if key is b'':
        if key_extra_len is 0:
            raise exc.MemcachedKeyNoneError("Key is empty")

        #  key is empty but there is some other component to key
        return

    if (const.MAX_KEY_LENGTH != 0 and
            len(key) + key_extra_len > const.MAX_KEY_LENGTH):
        raise exc.MemcachedKeyLengthError(
            "Key length is > %s" % const.MAX_KEY_LENGTH
        )
    if not const.REGEX_VALID_KEY.match(key):
        raise exc.MemcachedKeyCharacterError(
            'Control/space characters not allowed: {}'.format(key))
