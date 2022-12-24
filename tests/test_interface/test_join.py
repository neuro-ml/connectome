# from connectome import Source, meta, Transform, GroupBy
# from connectome.interface.blocks import Join
#
#
# def test_basic_join():
#     class A(Source):
#         @meta
#         def ids():
#             return tuple(range(10))
#
#         def mod(i):
#             return i % 3
#
#         def mod_key(i):
#             return i % 3
#
#     a = A()
#     b = a >> GroupBy('mod_key') >> Transform(mod_square=lambda id: id ** 2, mod=lambda id: id, __inherit__=True)
#     assert b.ids == (0, 1, 2)
#
#     c = Join(a, b, 'mod', lambda x: x[0])
#     assert c.ids == tuple(range(10))
#     for i in a.ids:
#         m = a.mod(i)
#         assert m == i % 3
#         assert m == c.mod(i)
#         assert a.mod_key(i) == c.mod_key(i)
#         assert c.mod_square(i) == m ** 2
