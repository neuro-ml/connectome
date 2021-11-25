from .base import Container, EdgesBag


class PipelineContainer(EdgesBag):
    def __init__(self, head: EdgesBag, *tail: Container):
        self.containers = [head, *tail]
        for layer in tail:
            head = layer.wrap(head)

        state = head.freeze()
        super().__init__(state.inputs, state.outputs, state.edges, state.context, virtual_nodes=state.virtual_nodes,
                         persistent_nodes=state.persistent_nodes)

    def wrap(self, container: EdgesBag) -> EdgesBag:
        return PipelineContainer(container, *self.containers)


class LazyPipelineContainer(Container):
    def __init__(self, *containers: Container):
        self.containers = containers

    def wrap(self, container: EdgesBag) -> EdgesBag:
        return PipelineContainer(container, *self.containers)
