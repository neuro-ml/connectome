import atexit
import warnings
import threading
import multiprocessing

from collections import defaultdict
from queue import Queue as ThreadQueue
from multiprocessing.pool import ThreadPool
from typing import Dict, Hashable, Sequence, Callable

executors = dict()
default_executor = None
thread_executor_lock = threading.Lock()


def get_thread_pool_executor(n_workers=None):
    global default_executor

    with thread_executor_lock:
        if n_workers is None:
            if default_executor is None:
                cpu_count = multiprocessing.cpu_count()
                default_executor = ThreadPool(cpu_count)
                executors[cpu_count] = default_executor
                atexit.register(default_executor.close)

            executor = default_executor
        elif n_workers not in executors:
            executor = ThreadPool(n_workers)
            executors[n_workers] = executor
            atexit.register(executor.close)
        else:
            executor = executors[n_workers]

        return executor


# TODO: move some of this logic to edge
class GraphTask:
    def __init__(self, evaluate: Callable, dependencies, rerun_on_error=False):
        assert callable(evaluate)
        self.evaluate = evaluate
        self.dependencies = dependencies
        self.rerun_locally = rerun_on_error

    def cleanup(self):
        pass


def get_direct_dependents(dep_graph: Dict):
    dependents = defaultdict(set)

    for name, task_parameters in dep_graph.items():
        for node_id in task_parameters.dependencies:
            assert node_id in dep_graph, node_id
            dependents[node_id].add(name)

    return dependents


def build_execution_state(dep_graph: Dict, cache: Dict = None):
    cache = cache or dict()
    dependencies = {k: dep_graph[k].dependencies.copy() for k in dep_graph}
    dependents = get_direct_dependents(dep_graph)

    waiting_data = {k: v for k, v in dependents.items() if v}
    ready = [k for k, v in dependencies.items() if not v]
    waiting = {k: v for k, v in dependencies.items() if v}

    state = {
        'dependencies': dependencies,
        'dependents': dependents,
        'waiting': waiting,
        'waiting_data': waiting_data,
        'cache': cache,
        'ready': ready,
        'running': set(),
        'finished': set(),
        'released': set(),
    }
    return state


def release_data(key, state, delete=True):
    if key in state['waiting_data']:
        assert not state['waiting_data'][key]
        del state['waiting_data'][key]

    state['released'].add(key)
    if delete:
        del state['cache'][key]


def process_finished_task(key: Hashable, state: Dict, output_nodes: Sequence[Hashable], delete=True):
    # remove finished tasks from following node's dependencies
    for dep in state['dependents'][key]:
        s = state['waiting'][dep]
        s.remove(key)
        # run node if it ready
        if not s:
            del state['waiting'][dep]
            state['ready'].append(dep)

    # iterate over data dependencies
    for dep in state['dependencies'][key]:
        if dep in state['waiting_data']:
            s = state['waiting_data'][dep]
            s.remove(key)

            if not s and dep not in output_nodes:
                release_data(dep, state, delete=delete)

        elif delete and dep not in output_nodes:
            release_data(dep, state, delete=delete)

    state['finished'].add(key)
    state['running'].remove(key)
    return state


# since object hash changes after being placed in a queue
def get_persistent_id(x):
    return id(x)


def execute_task(task_id, func, args):
    failed = False
    try:
        result = func(args)
    except BaseException as e:
        failed = True
        result = e

    return task_id, result, failed


def execute_graph_async(dep_graph: Dict, output_nodes, replace_by_persistent_ids=True, executor=None,
                        max_payload=None):
    executor = executor or get_thread_pool_executor()
    max_payload = max_payload or multiprocessing.cpu_count()
    if isinstance(executor, ThreadPool):
        queue = ThreadQueue()
    else:
        raise RuntimeError

    reversed_mapping = {}
    if replace_by_persistent_ids:
        persistent_graph = {}

        for node_id, node_task in dep_graph.items():
            new_id = get_persistent_id(node_id)
            dependencies = list(map(get_persistent_id, node_task.dependencies))
            persistent_graph[new_id] = GraphTask(node_task.evaluate, dependencies)
            reversed_mapping[new_id] = node_id

        dep_graph = persistent_graph
        output_nodes = [get_persistent_id(o) for o in output_nodes]

    cache = dict()
    state = build_execution_state(dep_graph, cache=cache)

    def deploy_task():
        current_task_id = state['ready'].pop()
        state['running'].add(current_task_id)
        current_task = dep_graph[current_task_id]
        cur_data = [state['cache'][dep] for dep in current_task.dependencies]
        executor.apply_async(execute_task, args=(current_task_id, current_task.evaluate, cur_data), callback=queue.put)

    def cleanup_waiting_tasks():
        while len(state['running']) != 0:
            t_id, _, _ = queue.get()
            process_finished_task(t_id, state, output_nodes)

        for t_id in list(state['waiting']) + list(state['ready']):
            dep_graph[t_id].cleanup()

    # deploy initial tasks
    while state['ready'] and len(state['running']) < max_payload:
        deploy_task()

    # main execution loop
    while state['waiting'] or state['ready'] or state['running']:
        task_id, result, failed = queue.get()
        if failed:
            exc = result
            if dep_graph[task_id].rerun_locally:
                warnings.warn(f'Try to rerun task {task_id}', RuntimeWarning)
                # try to rerun task if necessary
                task = dep_graph[task_id]
                try:
                    data = [state['cache'][dep] for dep in task.dependencies]
                    result = task.evaluate(data)
                except BaseException as e:
                    # inform waiting tasks
                    process_finished_task(task_id, state, output_nodes)
                    cleanup_waiting_tasks()
                    raise e

            else:
                # inform waiting tasks
                process_finished_task(task_id, state, output_nodes)
                cleanup_waiting_tasks()
                raise exc

        state['cache'][task_id] = result
        process_finished_task(task_id, state, output_nodes)
        while state['ready'] and len(state['running']) < max_payload:
            deploy_task()

    return dict((reversed_mapping.get(name, name), state['cache'][name]) for name in output_nodes)
