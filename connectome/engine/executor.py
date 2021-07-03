from abc import ABC, abstractmethod
from multiprocessing.pool import ThreadPool
from queue import Queue

from connectome.engine.base import Command


class Thunk:
    def __init__(self, parent):
        self.parent = parent
        self.error = False
        self.ready = False
        self.value = None


class Frame(Thunk):
    def __init__(self, stack, commands, parent):
        super().__init__(parent)
        self.stack = stack
        self.commands = commands


class Executor(ABC):
    def __init__(self, frame: Frame):
        self.frame = frame

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

    @abstractmethod
    def next_frame(self):
        pass

    @abstractmethod
    def enqueue_frame(self, x):
        pass

    @abstractmethod
    def clear(self):
        pass

    @abstractmethod
    def call(self, func, args):
        pass


class Backend:
    @abstractmethod
    def build(self, frame: Frame) -> Executor:
        pass


class Synchronous(Backend):
    def build(self, frame: Frame) -> Executor:
        return SequentialExecutor(frame)


class Threads(Backend):
    def build(self, frame: Frame) -> Executor:
        return ThreadPoolExecutor(frame, self.thunks)

    def __init__(self, n: int):
        self.n = n
        self.thunks = Queue()
        self.pool = ThreadPool(n, self._loop, (self.thunks,))
        self.pool.close()

    @staticmethod
    def _loop(thunks: Queue):
        while True:
            value = thunks.get()
            if value is None:
                thunks.task_done()
                break

            func, args, thunk, frames = value
            try:
                thunk.value = func(*args)
            # FIXME
            except BaseException:
                thunk.error = True

            thunk.ready = True
            thunks.task_done()
            frames.put(thunk.parent)

    def __del__(self):
        for _ in range(self.n):
            self.thunks.put(None)

        self.thunks.join()
        self.pool.join()


class SequentialExecutor(Executor):
    def __init__(self, frame: Frame):
        super().__init__(frame)
        self.frames = Queue()

    def clear(self):
        q = self.frames
        while not q.empty():
            assert q.get_nowait().ready

    def next_frame(self):
        self.frame = self.frames.get_nowait()

    def enqueue_frame(self, x):
        self.frames.put_nowait(x)

    def call(self, func, args):
        self.push(func(*args))


class ThreadPoolExecutor(Executor):
    def __init__(self, frame: Frame, queue: Queue):
        super().__init__(frame)
        self.frames, self.requests = Queue(), queue

    def clear(self):
        q = self.frames
        while not q.empty():
            assert q.get_nowait().ready

    def next_frame(self):
        self.frame = self.frames.get()

    def enqueue_frame(self, x):
        self.frames.put(x)

    def call(self, func, args):
        thunk = Thunk(self.frame)
        self.requests.put((func, args, thunk, self.frames))
        self.push_command((Command.AwaitThunk, thunk))
        self.next_frame()
