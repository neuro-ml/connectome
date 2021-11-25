from .base import Command
from .executor import Frame, Backend


# TODO: replace cache by a thunk tree
def execute(cmd, node, hashes, cache, backend: Backend):
    executor = backend.build(Frame([node], [(Command.Return,), (cmd,)], None))
    push, pop, peek = executor.push, executor.pop, executor.peek
    push_command, pop_command = executor.push_command, executor.pop_command
    next_frame, enqueue_frame = executor.next_frame, executor.enqueue_frame

    while True:
        if executor.frame.ready:
            next_frame()
            continue

        cmd, *args = pop_command()

        # return
        if cmd == Command.Return:
            assert not args
            assert len(executor.frame.stack) == 1, len(executor.frame.stack)
            executor.frame.value = pop()
            executor.frame.ready = True

            if executor.frame.parent is None:
                executor.clear()
                return executor.frame.value

            enqueue_frame(executor.frame.parent)
            next_frame()

        # communicate with edges
        elif cmd == Command.Send:
            node, iterator = args
            value = pop()
            try:
                request = iterator.send(value)

            except StopIteration as e:
                # clear the dependencies
                for n in node.parents:
                    hashes.evict(n)
                    cache.evict(n)
                # return value
                push(e.value)

            else:
                # must continue iteration
                push_command((cmd, node, iterator))
                push_command(request)
                push(node)

        # runs and caches `compute_hash`
        elif cmd == Command.ComputeHash:
            assert not args
            node = pop()
            if node in hashes:
                value = hashes[node]
                if isinstance(value, _CacheWaiter):
                    # restore state
                    push_command((cmd, *args))
                    push(node)
                    # remember to come back
                    value.add(executor.frame)
                    # switch context
                    next_frame()

                else:
                    push(value)
            else:
                hashes[node] = _CacheWaiter()
                push_command((Command.Store, hashes, node))
                push_command((Command.Send, node, node.edge.compute_hash()))
                push(None)

        # runs and caches `evaluate`
        elif cmd == Command.Evaluate:
            assert not args
            node = pop()

            if node in cache:
                value = cache[node]
                if isinstance(value, _CacheWaiter):
                    # restore state
                    push_command((cmd, *args))
                    push(node)
                    # remember to come back
                    value.add(executor.frame)
                    # switch context
                    next_frame()

                else:
                    push(value)
            else:
                cache[node] = _CacheWaiter()
                push_command((Command.Store, cache, node))
                push_command((Command.Send, node, node.edge.evaluate()))
                push(None)

        # requests
        elif cmd == Command.ParentHash:
            idx, = args
            node = pop()

            push_command((Command.Item, 0))
            push_command((Command.ComputeHash,))
            push(node.parents[idx])

        elif cmd == Command.ParentValue:
            idx, = args
            node = pop()

            push_command((Command.Evaluate,))
            push(node.parents[idx])

        elif cmd == Command.CurrentHash:
            assert not args
            push_command((Command.Item, 0))
            push_command((Command.ComputeHash,))

        elif cmd == Command.Payload:
            assert not args
            push_command((Command.Item, 1))
            push_command((Command.ComputeHash,))

        elif cmd == Command.Await:
            node = pop()
            push_command((Command.Tuple, len(args)))
            for arg in args:
                local = Frame([node], [(Command.Return,), arg], executor.frame)
                push_command((Command.AwaitThunk, local))
                enqueue_frame(local)

        elif cmd == Command.Call:
            pop()  # pop the node
            func, pos, kw = args
            executor.call(func, pos, kw)

        # utils
        elif cmd == Command.Store:
            storage, key = args
            parents = storage[key]
            assert isinstance(parents, _CacheWaiter), parents
            for parent in parents:
                enqueue_frame(parent)

            storage[key] = peek()

        elif cmd == Command.Item:
            key, = args
            push(pop()[key])

        elif cmd == Command.Tuple:
            n, = args
            push(tuple(pop() for _ in range(n)))

        elif cmd == Command.AwaitThunk:
            child, = args
            if child.ready:
                if child.error is not None:
                    raise child.error

                push(child.value)
            else:
                push_command((cmd, *args))
                next_frame()

        else:
            raise RuntimeError('Unknown command', cmd)


class _CacheWaiter(set):
    pass
