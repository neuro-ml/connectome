import sys
from concurrent.futures import Executor, Future

if sys.version_info[:2] < (3, 7):
    from queue import Queue as SimpleQueue
else:
    from queue import SimpleQueue


class SyncFuture:
    def __init__(self):
        self.ready = False
        self.error = self.value = None

    def done(self):
        return self.ready

    def result(self, timeout=None):
        assert self.ready
        if self.error is not None:
            raise self.error
        return self.value


class SyncExecutor(Executor):
    def submit(*args, **kwargs) -> Future:
        self, func, *args = args
        future = SyncFuture()
        future.ready = True
        try:
            future.value = func(*args, **kwargs)
        except BaseException as e:
            future.error = e

        return future


class Frame(SyncFuture):
    def __init__(self, stack, commands):
        super().__init__()
        self.stack = stack
        self.commands = commands


class AsyncLoop:
    def __init__(self, frame: Frame):
        self.frame = frame
        self.frames = SimpleQueue()

    def push(self, x):
        self.frame.stack.append(x)

    def pop(self):
        return self.frame.stack.pop()

    def peek(self):
        return self.frame.stack[-1]

    def push_command(self, x):
        self.frame.commands.append(x)

    def pop_command(self):
        return self.frame.commands.pop()

    def clear(self):
        q = self.frames
        while not q.empty():
            assert q.get_nowait().ready

    def dispose_frame(self):
        self.frame = self.frames.get_nowait()

    def next_frame(self):
        self.frames.put_nowait(self.frame)
        self.frame = self.frames.get_nowait()

    def enqueue_frame(self, x):
        self.frames.put_nowait(x)


DefaultExecutor = SyncExecutor()
