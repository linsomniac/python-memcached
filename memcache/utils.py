import binascii


def cmemcache_hash(key):
    return (
        (((binascii.crc32(key) & 0xffffffff)
          >> 16) & 0x7fff) or 1)
serverHashFunction = cmemcache_hash


def useOldServerHashFunction():
    """Use the old python-memcache server hash function."""
    global serverHashFunction
    serverHashFunction = binascii.crc32
