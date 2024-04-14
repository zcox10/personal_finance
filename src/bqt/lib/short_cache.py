import time


class ShortCache(object):
    """This is in memory dictionary cache used to manage some minor
    optimizations in BqT.

    Note: This is not a good implementation for holding large values as it
    doesn't do any cleanup of unused and expired keys.
    """

    def __init__(self, ttl=3600):
        # data structure is: dict(tuple(value, ttl in secs, unix time in secs))
        self._values = {}
        self._ttl = ttl

    def set(self, key, value, ttl=None):
        """Set a key and value

        Args:
            key (mixed, immutable): key to set the value for
            value (any): value to set
            ttl (int): TTL in seconds, if not provided default TTL from object
                is used
        """
        ttl = ttl or self._ttl
        self._values[key] = (value, ttl, time.time())

    def get(self, key, default=None):
        """Get a value using its key

        Args:
            key (mixed, immutable): key to set the value for
            default (any): value to return if the key is not set in the cache
        """
        if key not in self._values:
            return default
        if (time.time() - self._values[key][2]) > self._values[key][1]:
            del self._values[key]
            return default
        return self._values[key][0]

    def reset_ttl(self, key, ttl=None):
        """Reset the TTL and clock time for the given key

        Args:
            key (mixed, immutable): key to set the value for
            ttl (int): TTL in seconds, if not provided default TTL from object
                is used
        """
        self.set(key, self._values[key][0], ttl=ttl)
