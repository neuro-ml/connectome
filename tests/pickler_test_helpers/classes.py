class One:
    def __init__(self):
        self.x = 1

    def f(self, y):
        return self.x + y

    @staticmethod
    def s():
        raise ValueError('Some Err')

    @classmethod
    def c(cls):
        return cls().x

    @property
    def p(self):
        return self.x + 1


class A:
    x, y = 1, 1

    def __init__(self, x, y):
        self.a, self.b = x, y

    def f(self, x):
        return x + 1 + self.x

    @classmethod
    def __getversion__(cls):
        return cls.x


class B:
    x, y = 1, 1

    def __init__(self, x, y):
        self.a, self.b = x, y

    def f(self, x):
        return x + 2 + self.x

    @classmethod
    def __getversion__(cls):
        return cls.x
