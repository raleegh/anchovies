from anchovies.sdk import TaskStore


class HiveTaskStore(TaskStore): 
    '''
    Unlike the `TaskStore`, this store puts all task logs in a hive-like
    directory structure in order to make adhoc analysis a little easier.
    '''
    #TODO: complete

