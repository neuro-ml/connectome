from abc import ABC, abstractmethod
from queue import Queue


class Frame:
    def __init__(self, stack, commands, parent):
        self.stack = stack
        self.commands = commands
        self.ready = False
        self.value = None
        self.parent = parent


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
    def call(self, func, args):
        pass


class SequentialExecutor(Executor):
    def __init__(self, frame: Frame):
        super().__init__(frame)
        self.queue = Queue()

    def next_frame(self):
        self.frame = self.queue.get_nowait()

    def enqueue_frame(self, x):
        self.queue.put_nowait(x)

    def call(self, func, args):
        self.push(func(*args))
