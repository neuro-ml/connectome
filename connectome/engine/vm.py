from concurrent.futures import Executor

from .base import Command
from .executor import Frame, AsyncLoop


# TODO: replace cache by a thunk tree
def execute(cmd, node, hashes, cache, executor: Executor):
    root = Frame([node], [(Command.Return,), (cmd,)])
    loop = AsyncLoop(root)
    push, pop, peek = loop.push, loop.pop, loop.peek
    push_command, pop_command = loop.push_command, loop.pop_command
    next_frame, enqueue_frame = loop.next_frame, loop.enqueue_frame
    dispose_frame = loop.dispose_frame

    while True:
        assert not loop.frame.ready
        cmd, *args = pop_command()

        # return
        if cmd == Command.Return:
            assert not args
            assert len(loop.frame.stack) == 1, len(loop.frame.stack)
            value = pop()
            if loop.frame is root:
                loop.clear()
                return value

            loop.frame.value = value
            loop.frame.ready = True
            dispose_frame()

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
                if value is _CACHE_SENTINEL:
                    # restore state
                    push_command((cmd, *args))
                    push(node)
                    # switch context
                    next_frame()

                else:
                    push(value)
            else:
                hashes[node] = _CACHE_SENTINEL
                push_command((Command.Store, hashes, node))
                push_command((Command.Send, node, node.edge.compute_hash()))
                push(None)

        # runs and caches `evaluate`
        elif cmd == Command.Evaluate:
            assert not args
            node = pop()

            if node in cache:
                value = cache[node]
                if value is _CACHE_SENTINEL:
                    # restore state
                    push_command((cmd, *args))
                    push(node)
                    # switch context
                    next_frame()

                else:
                    push(value)
            else:
                cache[node] = _CACHE_SENTINEL
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
                local = Frame([node], [(Command.Return,), arg])
                push_command((Command.AwaitFuture, local))
                enqueue_frame(local)

        elif cmd == Command.Call:
            pop()  # pop the node
            func, pos, kw = args
            push_command((Command.AwaitFuture, executor.submit(func, *pos, **kw)))
            next_frame()

        # utils
        elif cmd == Command.Store:
            storage, key = args
            sentinel = storage[key]
            assert sentinel is _CACHE_SENTINEL, sentinel

            storage[key] = peek()

        elif cmd == Command.Item:
            key, = args
            push(pop()[key])

        elif cmd == Command.Tuple:
            n, = args
            push(tuple(pop() for _ in range(n)))

        elif cmd == Command.AwaitFuture:
            child, = args
            if child.done():
                push(child.result())
            else:
                push_command((cmd, *args))
                next_frame()

        else:
            raise RuntimeError('Unknown command', cmd)  # pragma: no cover


_CACHE_SENTINEL = object()
