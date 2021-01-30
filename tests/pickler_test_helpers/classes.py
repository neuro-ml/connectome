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
