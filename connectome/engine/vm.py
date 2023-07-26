from collections import deque

from .base import Command


# TODO: replace cache by a thunk tree
def execute(cmd, node, hashes, cache):
    stack, commands = deque([node]), deque([(Command.Return,), (cmd,)])
    push, pop, peek = stack.append, stack.pop, lambda: stack[-1]
    push_command, pop_command = commands.append, commands.pop

    while True:
        cmd, *args = pop_command()

        # return
        if cmd == Command.Return:
            assert len(stack) == 1, len(stack)
            return pop()

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
                push(hashes[node])
            else:
                push_command((Command.Store, hashes, node))
                push_command((Command.Send, node, node.edge.compute_hash()))
                push(None)

        # runs and caches `evaluate`
        elif cmd == Command.Evaluate:
            assert not args
            node = pop()
            if node in cache:
                push(cache[node])
            else:
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
            push_command((Command.Tuple, node, len(args), list(args)))

        elif cmd == Command.Call:
            pop()  # pop the node
            func, pos, kw = args
            push(func(*pos, **kw))

        # utils
        elif cmd == Command.Store:
            storage, key = args
            assert key not in storage
            storage[key] = peek()

        elif cmd == Command.Item:
            key, = args
            push(pop()[key])

        elif cmd == Command.Tuple:
            node, n, requests = args
            if not requests:
                push(tuple(pop() for _ in range(n)))
            else:
                request = requests.pop()
                push_command((Command.Tuple, node, n, requests))
                push_command(request)
                push(node)

        else:
            raise RuntimeError('Unknown command', cmd)  # pragma: no cover
