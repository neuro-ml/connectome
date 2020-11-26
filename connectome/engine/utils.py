class ExpirationCache:
    def __init__(self, counts, cache=None):
        self.counts = counts
        self.cache = cache or {}

    def __setitem__(self, key, value):
        assert key in self.counts
        assert key not in self.cache
        self.cache[key] = value

    def __getitem__(self, key):
        counts = self.counts[key]
        value = self.cache[key]

        assert counts
        counts -= 1
        if counts < 0:
            del self.cache[key]
            del self.counts[key]
        else:
            self.counts[key] = counts
        return value

    def __contains__(self, key):
        return key in self.cache
