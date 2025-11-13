from .tasks import * 


class Overseer(Task): 
    '''A Task that oversees other tasks.'''
    def __init__(self):
        super().__init__(target=self.monitor, daemon=True)

    def oversee(self, task: Task): 
        '''An alias for `also_promise()`.'''
        if isinstance(task, Overseer): 
            return
        return self.also_promise(task)
    
    def notify_done(self, task: Task): 
        '''Remove a task from the Overseer.'''
        if task in self.promises: 
            self.promises.remove(task)

    def monitor(self): 
        '''Monitor underlying tasks.'''
        while not self.stopped.wait(10):
            for fut in self.promises:
                if fut.has_checkin:
                    fut.checkin()


CURRENT_OVERSEER = Overseer().start_or_enter()


def overseer() -> Overseer:
    '''Retrieve the contextual Overseer.'''
    return CURRENT_OVERSEER
    # at a future date, may change to be a context var :)
