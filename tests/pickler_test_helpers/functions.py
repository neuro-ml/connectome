def identity(x):
    return x


def scope():
    def identity(x):
        return x

    return identity


nested_identity = scope()
