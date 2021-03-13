class Prepared:
    pass


class ComputableHash(Prepared):
    def __init__(self, precompute, func):
        self.precompute = precompute
        self.func = func
