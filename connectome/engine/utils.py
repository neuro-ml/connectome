class ExpirationCache:
    def __init__(self, counts, inputs, cache=None):
        self.counts = counts
        self.inputs = inputs
        self.cache = cache or {}

    def __setitem__(self, key, value):
        if key not in self.inputs:
            assert key in self.counts

        assert key not in self.cache
        self.cache[key] = value

    def __getitem__(self, key):
        assert self.counts[key]
        value = self.cache[key]
        self.counts[key] -= 1
        if self.counts[key] <= 0:
            del self.cache[key]
            del self.counts[key]
        return value

    def __contains__(self, key):
        return key in self.cache
