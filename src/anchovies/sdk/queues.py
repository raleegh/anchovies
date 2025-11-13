import threading as th
from queue import Queue as BaseQueue
from queue import Full, Empty
from .tasks import Task


__all__ = ['Queue', 'Full', 'Empty']


class Queue(BaseQueue, Task): 
    '''A queue that can be naturally iterated.'''
    def __init__(self, maxsize = 0):
        Task.__init__(self)
        BaseQueue.__init__(self, maxsize)

    def __iter__(self): 
        if self.stopped is None:
            yield from iter(self.get, StopIteration)
            return
        backoff = 0
        while not self.stopped.is_set():
            try:
                val = self.get_nowait()
                if val is StopIteration:
                    break
                yield val
                self.task_done()
                backoff = 0
            except Empty: 
                self.stopped.wait(0.01 * (2 ** backoff))
                backoff = min(backoff + 1, 10)
    
    def join_or_exit(self): 
        self.put(StopIteration)
        super().join_or_exit()

    def put(self, i, block=True, timeout=None):
        '''Add an item to the queue.'''
        if self.stopped is None: 
            return super().put(i, block=block, timeout=timeout)
        while not self.stopped.is_set():
            try:
                return super().put_nowait(i)
            except Full:
                pass

    def get(self): 
        '''Get an item from the queue.'''
        while not self.stopped.is_set(): 
            try: 
                i = super().get_nowait()
                self.task_done()
                return i
            except Empty: 
                pass
