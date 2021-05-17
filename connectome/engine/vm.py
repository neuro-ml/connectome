from queue import Queue

from .base import Command


class Frame:
    def __init__(self, stack, commands, parent):
        self.stack = stack
        self.commands = commands
        self.ready = False
        self.value = None
        self.parent = parent


class _CacheWaiter(set):
    pass


# TODO: replace cache by a thunk tree
def execute(cmd, node, hashes, cache):
    def next_frame():
        nonlocal frame, commands, stack
        new = frames_queue.get_nowait()
        commands, stack, frame = new.commands, new.stack, new

    def enqueue_frame(x):
        frames_queue.put_nowait(x)

    frames_queue = Queue()
    stack = [node]
    commands = [(Command.Return,), (cmd,)]
    frame = Frame(stack, commands, None)

    while True:
        if frame.ready:
            next_frame()
            continue

        cmd, *args = commands.pop()

        # return
        if cmd == Command.Return:
            assert not args
            assert len(stack) == 1, len(stack)
            frame.value = stack.pop()
            frame.ready = True

            if frame.parent is None:
                while not frames_queue.empty():
                    assert frames_queue.get_nowait().ready
                return frame.value

            enqueue_frame(frame.parent)
            next_frame()

        # communicate with edges
        elif cmd == Command.Send:
            node, iterator = args
            value = stack.pop()
            try:
                request = iterator.send(value)

            except StopIteration as e:
                # clear the dependencies
                for n in node.parents:
                    hashes.evict(n)
                    cache.evict(n)
                # return value
                stack.append(e.value)

            else:
                # must continue iteration
                commands.append((cmd, node, iterator))
                commands.append(request)
                stack.append(node)

        # runs and caches `compute_hash`
        elif cmd == Command.ComputeHash:
            assert not args
            node = stack.pop()
            if node in hashes:
                value = hashes[node]
                if isinstance(value, _CacheWaiter):
                    # restore state
                    commands.append((cmd, *args))
                    stack.append(node)
                    # remember to come back
                    value.add(frame)
                    # switch context
                    next_frame()

                else:
                    stack.append(value)
            else:
                hashes[node] = _CacheWaiter()
                commands.append((Command.Store, hashes, node))
                commands.append((Command.Send, node, node.edge.compute_hash()))
                stack.append(None)

        # runs and caches `evaluate`
        elif cmd == Command.Evaluate:
            assert not args
            node = stack.pop()

            if node in cache:
                value = cache[node]
                if isinstance(value, _CacheWaiter):
                    # restore state
                    commands.append((cmd, *args))
                    stack.append(node)
                    # remember to come back
                    value.add(frame)
                    # switch context
                    next_frame()

                else:
                    stack.append(value)
            else:
                cache[node] = _CacheWaiter()
                commands.append((Command.Store, cache, node))
                commands.append((Command.Send, node, node.edge.evaluate()))
                stack.append(None)

        # requests
        elif cmd == Command.ParentHash:
            idx, = args
            node = stack.pop()

            commands.append((Command.Item, 0))
            commands.append((Command.ComputeHash,))
            stack.append(node.parents[idx])

        elif cmd == Command.ParentValue:
            idx, = args
            node = stack.pop()

            commands.append((Command.Evaluate,))
            stack.append(node.parents[idx])

        elif cmd == Command.CurrentHash:
            assert not args
            commands.append((Command.Item, 0))
            commands.append((Command.ComputeHash,))

        elif cmd == Command.Payload:
            assert not args
            commands.append((Command.Item, 1))
            commands.append((Command.ComputeHash,))

        elif cmd == Command.Await:
            node = stack.pop()
            commands.append((Command.Tuple, len(args)))
            for arg in args:
                local = Frame([node], [(Command.Return,), arg], frame)
                commands.append((Command.AwaitFrame, local))
                enqueue_frame(local)

        # utils
        elif cmd == Command.Store:
            storage, key = args
            parents = storage[key]
            assert isinstance(parents, _CacheWaiter), parents
            for parent in parents:
                enqueue_frame(parent)

            storage[key] = stack[-1]

        elif cmd == Command.Item:
            key, = args
            stack.append(stack.pop()[key])

        elif cmd == Command.Tuple:
            n, = args
            value = tuple(stack.pop() for _ in range(n))
            stack.append(value)

        elif cmd == Command.AwaitFrame:
            child, = args
            if child.ready:
                stack.append(child.value)
            else:
                commands.append((cmd, *args))
                next_frame()

        else:
            raise RuntimeError('Unknown command', cmd)
