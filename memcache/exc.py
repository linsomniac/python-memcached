class Error(Exception):
    pass


class ConnectionDeadError(Exception):
    pass


class MemcachedKeyError(Exception):
    pass


class MemcachedKeyLengthError(MemcachedKeyError):
    pass


class MemcachedKeyCharacterError(MemcachedKeyError):
    pass


class MemcachedKeyNoneError(MemcachedKeyError):
    pass


class MemcachedKeyTypeError(MemcachedKeyError):
    pass


class MemcachedStringEncodingError(Exception):
    pass
