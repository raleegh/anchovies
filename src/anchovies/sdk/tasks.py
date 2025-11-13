'''Asynchronous tasks & services.

Ahead of the 0.1.0 release, I want to address a major 
problem I'm running into with anchovies: the threading module
provides poor support for the actual orchestration of tasks.

A Task is an object that has a `start()` and `join()` method.
Tasks can be stopped via `stop()`. Additionally, Tasks can be 
associated to each other (with parent-child relationship) via 
`also_promise()`.

Using this API will ensure all the microservices and threading
processes communicate in the same way. Additionally, this should 
streamline shared scheduling of services & even handling.
'''
import threading as th
import uuid
import logging; logger = logging.getLogger(__name__)
from abc import ABC
from typing import Any, Callable, Optional, Self


__all__ = ['Task', 'Callback']


class NamedEvent(th.Event):
    '''Allow an event to have a name.'''
    def __init__(self, name: str):
        super().__init__()
        self.name = name
    
    def __str__(self):
        return self.name


class Task(ABC):
    '''An asynchronous task.

    Wrap a worker function in a Task to convert it into 
    an asynchronous task.

    Usage: 
    ```
    task = Task(target=sleep)
    task.start_or_enter()
    task.join_or_exit()
    ```

    You can also use a Context Manager API on Task:
    ```
    with Task(target=sleep, args=(3,)): 
        ...
        # if this doesn't take longer than 3,
        #   the task will block until 3 seconds
    ```
    '''
    def __init__(
        self,
        *,
        target: Optional[Callable]=None,
        args=(),
        kwds: dict=None,
        timeout: float=None,
        daemon=False,
        construct=False,
    ):
        self.target = target
        self.args = args
        self.kwds = kwds or {}
        self.daemon = daemon
        self.__construct = construct
        self.__promise = None
        self.timeout = timeout
        self.started = NamedEvent('on_start')
        self.stopped = NamedEvent('on_stop')
        self.succeeded = NamedEvent('on_success')
        self.crashed = NamedEvent('on_crash')
        self.exception = None
        self.result = None
        self._promises: 'set[Task]' = set()
        self.id = self.make_id()
        self._enter_hit_count = 0
        self.overseer = None
        self.promised_by: Optional[Self] = None

    def make_id(self): 
        '''Create the identifier for this Task.'''
        return str(uuid.uuid4())[:3]

    @property
    def promises(self): 
        '''These are the children promises for this Task.'''
        return self._promises
    
    @property
    def is_construct(self): 
        '''A Task can be assembled with no underlying promise.
        
        This flag tell us that this Task behaves that way.
        '''
        return self.target is None or self.__construct
    
    @property
    def thread_label(self): 
        '''The value passed to Thread.name.'''
        return f'{self} ({self.target.__name__})'

    def get_promise_constructor(self): 
        '''Return the target, args, kwds to schedule.'''
        return (
            self.target,
            self.args, 
            self.kwds, 
        )

    def execute_wrapped_target(self):
        '''Run the `target` promise.'''
        tgt, args, kwds = self.get_promise_constructor()
        try:
            self.result = tgt(*args, **kwds)
            for fut in self.promises:
                fut.join_or_exit()
            self.succeeded.set()
            self.stopped.set()
        except Exception as e: 
            self.exception = e
            self.crash()
            raise e
        except: 
            self.crash()
            raise
        finally: 
            self.detach_overseer()

    def make_promise(self):
        '''Assemble the promise.'''
        self.__promise = th.Thread(
            target=self.execute_wrapped_target, 
            daemon=self.daemon,
        )
        self.__promise.name = repr(self)

    def attach_overseer(self): 
        from .overseers import overseer
        self.overseer = overseer()
        self.overseer.oversee(self)

    def detach_overseer(self): 
        if self.overseer is None:
            return
        self.overseer.notify_done(self)
        self.overseer = None

    def start_or_enter(self): 
        '''Schedule the task for execution.'''
        try:
            self.started.set()
            self.attach_overseer()
            if self.is_construct: 
                return
            self.make_promise()
            self.__promise.start()
            return self
        except: 
            self.crash()

    def start(self):
        '''An alias for `start_or_enter()`.'''
        return self.start_or_enter()
    
    def enter(self): 
        '''An alias for `start_or_enter()`.'''
        return self.start_or_enter()
    
    def maybe_start_or_enter(self): 
        '''If the task hasn't been started, start it.'''
        if not self.started.is_set():
            self.start_or_enter()
        return self
    
    def join_or_exit(self) -> Any: 
        '''Wait for the promise to finish executing, return the result.
        
        A timeout can be set via the `timeout` property. This Task waits 
        on all it's children tasks as well.
        '''
        if self.is_construct: 
            return self.result
        try:
            return self.actually_join_or_exit()
        except:
            self.crash()
            raise

    def actually_join_or_exit(self): 
        '''An internal method to run the join() logic.'''
        self.__promise.join(self.timeout)
        if self.is_alive() and not self.crashed.is_set(): 
            raise TimeoutError(
                f'{self} timed out waiting for {self.timeout:,}s.'
            )
        self.unlink_promise()
        self.stopped.set()
        if self.exception: 
            raise self.exception
        return self.result

    def join(self): 
        '''An alias for `join_or_exit()`.'''
        return self.join_or_exit()    

    def exit(self): 
        '''An alias for `join_or_exit()`.'''
        return self.join_or_exit()

    def maybe_join_or_exit(self): 
        if self.stopped.is_set(): 
            return self.result
        return self.join_or_exit()

    def is_alive(self): 
        '''Check if the underlying promise is alive.'''
        return self.started.is_set() and not self.stopped.is_set()

    def crash(self): 
        '''Stop the execution of this Task & it's children.'''
        self.crashed.set()
        self.stopped.set()
        for fut in self.promises: 
            fut.crash()
        self.actually_join_or_exit()

    def also_promise(self, task: Self): 
        '''Attach a child Task to this Task.
        
        Tasks associated in this way will be shutdown/crashed
        if this Task is also crashed.
        '''
        task.promised_by = self
        task.maybe_start_or_enter()
        self.promises.add(task)
        return task
    
    def unlink_promise(self): 
        '''If this Task was invoked as a promise, remove the bond.'''
        if self.promised_by: 
            self.promised_by.promises.remove(self)

    def on_event(self, event: th.Event, callback: Callable): 
        '''Add a callback to run on an arbitrary event.'''
        t = Callback(self, event, callback)
        self.also_promise(t)
        return t

    def on_start(self, callback): 
        '''Add a promise to the start event.'''
        return self.on_event(self.started, callback)

    def on_stop(self, callback): 
        '''Add a promise to the stop event.'''
        return self.on_event(self.stopped, callback)

    def on_crash(self, callback): 
        '''Add a promise to the crash event.'''
        return self.on_event(self.crashed, callback)

    def on_success(self, callback): 
        '''Add a promise to the success event.'''
        return self.on_event(self.succeeded, callback)

    def sleep(self, seconds: float):
        '''Wait for the specified seconds, so long as not stopped.'''
        self.stopped.wait(seconds)

    def checkin(self): 
        '''Monitoring/oversight function.
        
        The overseer will periodically checkin on this Task.
        The logic here can be used to provide status logs/updates
        related to the progress of this task. In order to have 
        this function called, you must also change the `has_checkin` 
        property.
        '''
        ...

    @property
    def has_checkin(self):
        '''Tell the overseer if any monitoring needs to be done.'''
        return False

    def __enter__(self): 
        '''Start the promise. Alias for `start()`.'''
        self._enter_hit_count += 1
        return self.maybe_start_or_enter()
    
    def __exit__(self, *args): 
        '''Join the promise. Alias for `join()`.'''
        self._enter_hit_count -= 1
        if self._enter_hit_count == 0:
            self.maybe_join_or_exit()
    
    def __repr__(self): 
        return f'Task+{self.id}'
    
    def __str__(self): 
        return repr(self)
    

class Callback(Task): 
    '''A Task created through the Task callback system:
    ```
    task = Task(some_long_running).start()
    callback_task = task.on_success(some_cleanup_function)
    ```
    '''
    def __init__(self, task: Task, event, callback, **kwds):
        self.callback = callback
        callback = self.finalize_callback(event, callback)
        kwds['target'] = callback
        super().__init__(**kwds)
        self._event = event

    def __repr__(self):
        f'Callback({self.owned_by} --{self._event}-> {self.callback.__name__})'

    @property
    def thread_label(self): 
        return repr(self)
    
    def finalize_callback(self, event: th.Event, callback: Callable): 
        '''Wrap the callback with a waiter condition at the start.'''
        def wrapper(): 
            event.wait()
            callback(self)
        return wrapper
    
