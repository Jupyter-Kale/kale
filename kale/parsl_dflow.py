
#std lib
import concurrent.futures as cf

# 3rd party
from parsl import DataFlowKernel

# Kale
import kale.workflow_objects as wo

class KaleDFK(DataFlowKernel):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fu_to_func = dict()
        self.fu_to_task = dict()

        # A workflow must be created manually
        # by calling self.new_workflow(name)
        self.workflow_created = False

        # Store names and funcs of deps,
        # indexed by task_id.
        self.dep_funcs = dict()
        self.dep_names = dict()

    def parse_args(self, args, kwargs):
        """Replace any futures in args or kwargs with their corresponding Task at definition time
        (which will be replaced by the future's result by Kale at execution time)."""
        new_args = []
        for arg in args:
            if isinstance(arg, cf.Future):
                new_args.append(self.fu_to_task[arg])
            else:
                new_args.append(arg)

        new_kwargs = dict()
        for key, val in kwargs.items():
            if isinstance(val, cf.Future):
                new_kwargs[key] = self.fu_to_task[val]
            else:
                new_kwargs[key] = val

        return new_args, new_kwargs


    def new_workflow(self, name):
        self.kale_workflow = wo.Workflow(name=name)
        self.workflow_created = True

    def submit(self, func, *args, **kwargs):
        if self.workflow_created:
            return self.inner_submit(func, *args, **kwargs)
        else:
            raise Exception("Call KaleDFK.new_workflow(name) before adding tasks.")

    def launch_task(self, task_id, executable, *args, **kwargs):
        """Override launch_task to do nothing at definition time."""
        return gen_empty_future()

    def inner_submit (self, func, *args, **kwargs):
        from parsl.dataflow.states import States
        from parsl.dataflow.futures import AppFuture
        ''' Add task to the dataflow system.

        If all deps are met :
              send to the runnable queue
              and launch the task
        Else:
              post the task in the pending queue

        Returns:
               (AppFuture) [DataFutures,]
        '''

        # print("""Inner submit
        # self: {}
        # func: {}
        # args: {}
        # kwargs: {}
        # """.format(self,func, args, kwargs))


        task_id = self.task_count
        self.task_count += 1

        # Extract task & dependency info
        task_name = func.__name__

        dep_cnt, depends = super()._count_all_deps(task_id, args, kwargs)

        #print("task_id = {}".format(task_id))
        #print("args = {}".format(args))
        #print("kwargs = {}".format(kwargs))
        #print("dep_cnt = {}".format(dep_cnt))
        #print("depends = {}".format(depends))


        self.dep_funcs[task_id] = [self.fu_to_func[fu] for fu in depends]
        self.dep_names[task_id] = [dep.__name__ for dep in self.dep_funcs[task_id]]

        #print("dep funcs = {}".format(self.dep_funcs[task_id]))
        #print("dep names = {}".format(self.dep_names[task_id]))

        #dep_cnt  = self._count_deps(dep ends, task_id)
        task_def = { 'depends'    : depends,
                     'func'       : func,
                     'args'       : args,
                     'kwargs'     : kwargs,
                     'callback'   : None,
                     'dep_cnt'    : dep_cnt,
                     'exec_fu'    : None,
                     'status'     : States.unsched,
                     'app_fu'     : None  }

        if task_id in self.tasks:
            raise DuplicateTaskError("Task {0} in pending list".format(task_id))
        else:
            self.tasks[task_id] = task_def

        # Extract stdout and stderr to pass to AppFuture:
        task_stdout = kwargs.get('stdout', None)
        task_stderr = kwargs.get('stderr', None)

        if dep_cnt == 0 :
            # Set to running
            new_args, kwargs, exceptions = self.sanitize_and_wrap(task_id, args, kwargs)
            if not exceptions:
                self.tasks[task_id]['exec_fu'] = self.launch_task(task_id, func, *new_args, **kwargs)
                self.tasks[task_id]['app_fu']  = AppFuture(self.tasks[task_id]['exec_fu'],
                                                           tid=task_id,
                                                           stdout=task_stdout,
                                                           stderr=task_stderr)
                self.tasks[task_id]['status']  = States.running
            else:
                self.tasks[task_id]['exec_fu'] = None
                app_fu = AppFuture(self.tasks[task_id]['exec_fu'],
                                   tid=task_id,
                                   stdout=task_stdout,
                                   stderr=task_stderr)
                app_fu.set_exception(DependencyError(exceptions, "Failures in input dependencies", None))
                self.tasks[task_id]['app_fu']  = app_fu
                self.tasks[task_id]['status']  = States.dep_fail
        else:
            # Send to pending, create the AppFuture with no parent and have it set
            # when an executor future is available.
            self.tasks[task_id]['app_fu']  = AppFuture(None, tid=task_id,
                                                       stdout=task_stdout,
                                                       stderr=task_stderr)
            self.tasks[task_id]['status']  = States.pending

        #logger.debug("Task:%s Launched with AppFut:%s", task_id, task_def['app_fu'])


        fu = task_def['app_fu'] # This was the return value

        ## End of Parsl code

        self.fu_to_func[fu] = func

        # Replace futures with Tasks in args for Task definition
        new_args, new_kwargs = self.parse_args(args, kwargs)

        # Create Task
        task = wo.PythonFunctionTask(
            name=task_name,
            func=func,
            args=new_args,
            kwargs=new_kwargs
        )

        self.fu_to_task[fu] = task

        # This doesn't work if multiple tasks have the same name (common w/ Parsl definition)
        #dep_tasks = [
        #    self.kale_workflow.get_task_by_name(dep_name)
        #    for dep_name in self.dep_names[task_id]
        #]

        dep_tasks = [
            self.fu_to_task[depend]
            for depend in depends
        ]

        # Add to workflow
        self.kale_workflow.add_task(
            task,
            dependencies=dep_tasks
        )

        return fu

def gen_empty_future():
    f = cf.Future()
    f._state = 'FINISHED'
    return f
