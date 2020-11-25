from collections import defaultdict

from .base import EdgesBag
from ..engine.base import BoundEdge, Node
from ..engine.edges import ProductEdge
from ..engine.interface import ValueEdge


def make_static_product(layer: EdgesBag, keys, output_names):
    output_groups = defaultdict(list)
    edges = []

    for key in keys:
        params = layer.prepare()
        inp, = params.inputs
        edges.extend(params.edges)
        edges.append(BoundEdge(ValueEdge(key), [], inp))

        for output in params.outputs:
            if output.name in output_names:
                output_groups[output.name].append(output)

    outputs = {}
    for name, nodes in output_groups.items():
        assert len(nodes) == len(keys)
        out = Node(name)
        outputs[name] = out
        edges.append(BoundEdge(ProductEdge(len(nodes)), nodes, out))

    return edges, outputs
