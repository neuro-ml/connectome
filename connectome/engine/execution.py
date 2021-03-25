from collections import defaultdict
from typing import Dict, Hashable, Sequence

from queue import Queue as ThreadQueue
from multiprocessing.pool import ThreadPool


class BaseExecutor(object):
    pass


class ExecutionState(object):
    pass


def get_direct_dependencies(dep_graph: Dict, task_name: Hashable):
    task_info = dep_graph[task_name]
    # check first object to be edge
    assert callable(task_info[0])
    deps = task_info[1:]

    for d in deps:
        assert d in dep_graph, d
    return list(deps)


def get_direct_dependents(dep_graph: Dict):
    dependents = defaultdict(set)

    for name, dependencies in dep_graph.items():
        assert callable(dependencies[0])
        dependencies = dependencies[1:]

        for v in dependencies:
            assert v in dep_graph, v
            dependents[v].add(name)

    return dependents


def build_execution_state(dep_graph, cache: Dict = None):
    cache = cache or dict()
    dependencies = {k: get_direct_dependencies(dep_graph, k) for k in dep_graph}
    dependents = get_direct_dependents(dep_graph)

    waiting = {k: v for k, v in dependencies.items()}
    waiting_data = {k: v for k, v in dependents.items() if v}
    ready = [k for k, v in waiting.items() if not v]
    waiting = {k: v for k, v in dependencies.items() if v}

    state = {
        "dependencies": dependencies,
        "dependents": dependents,
        "waiting": waiting,
        "waiting_data": waiting_data,
        "cache": cache,
        "ready": ready,
        "running": set(),
        "finished": set(),
        "released": set(),
    }
    return state


def release_data(key, state, delete=True):
    if key in state["waiting_data"]:
        assert not state["waiting_data"][key]
        del state["waiting_data"][key]

    state["released"].add(key)

    if delete:
        del state["cache"][key]


def process_finished_task(key: Hashable, state: Dict, output_nodes: Sequence[Hashable], delete=True):
    # remove finished tasks from following node's dependencies
    for dep in state["dependents"][key]:
        s = state["waiting"][dep]
        s.remove(key)
        # run node if it ready
        if not s:
            del state["waiting"][dep]
            state["ready"].append(dep)

    # iterate over data dependencies
    for dep in state["dependencies"][key]:
        if dep in state["waiting_data"]:
            s = state["waiting_data"][dep]
            s.remove(key)

            if not s and dep not in output_nodes:
                release_data(dep, state, delete=delete)

        elif delete and dep not in output_nodes:
            release_data(dep, state, delete=delete)

    state["finished"].add(key)
    state["running"].remove(key)
    return state


# since object hash changes after being placed in a queue
def get_persistent_id(x):
    return id(x)


def execute_task(task_id, func, args):
    # TODO: give more info
    failed = False
    try:
        result = func(args)
    except BaseException as e:
        failed = True
        result = None

    return task_id, result, failed


# TODO: decompose this function
def execute_graph_async(dep_graph: Dict, output_nodes: Sequence, add_persistent_ids=True, num_workers=4):
    # TODO: move it to a separate provider
    # TODO: add cleanup
    executor = ThreadPool(num_workers)
    queue = ThreadQueue()

    reversed_mapping = {}
    if add_persistent_ids:
        persistent_graph = {}

        for node, node_info in dep_graph.items():
            dependencies = list(map(get_persistent_id, node_info[1:]))
            node_id = get_persistent_id(node)
            persistent_graph[node_id] = (node_info[0], *dependencies)
            reversed_mapping[node_id] = node

        dep_graph = persistent_graph
        output_nodes = [get_persistent_id(o) for o in output_nodes]

    cache = dict()
    state = build_execution_state(dep_graph, cache=cache)

    def deploy_task():
        current_task_id = state["ready"].pop()
        state["running"].add(current_task_id)

        task_dependencies = get_direct_dependencies(dep_graph, current_task_id)
        data = [state["cache"][dep] for dep in task_dependencies]
        task_func, task_arg_names = dep_graph[current_task_id][0], dep_graph[current_task_id][1:]
        executor.apply_async(execute_task, args=(current_task_id, task_func, data), callback=queue.put)

    # deploy initial tasks
    # TODO: add max worker capacity
    while state["ready"]:
        deploy_task()

    # main execution loop
    while state["waiting"] or state["ready"] or state["running"]:
        key, res, failed = queue.get()
        if failed:
            # TODO: put exception handling here (wait for running entities and propagate errors)
            raise RuntimeError()

        state["cache"][key] = res
        process_finished_task(key, state, output_nodes)
        # TODO: add max worker capacity
        while state["ready"]:
            deploy_task()

    return dict((reversed_mapping.get(name, name), state['cache'][name]) for name in output_nodes)
