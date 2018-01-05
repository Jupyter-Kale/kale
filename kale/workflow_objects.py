# Oliver Evans
# August 7, 2017

# stdlib
import os
import time
import yaml
from datetime import datetime
import concurrent.futures as cf

# 3rd party
import bqplot as bq
import networkx
import ipywidgets as ipw
import traitlets
import fireworks as fw
from fireworks.core.rocket_launcher import rapidfire
from parsl import ThreadPoolExecutor, DataFlowKernel

# local
import kale.batch_jobs
from kale.parsl_wrappers import parsl_wrap, parsl_func_after_futures

# TODO - Convert print statements to logging statements

# TODO - update all instances of default arguments set to a mutable e.g.; [], {}
class WorkerPool(traitlets.HasTraits):
    """Pool of workers which can execute jobs."""

    futures = traitlets.List()
    workers = traitlets.List()
    wf_executor = traitlets.Unicode()
    name = traitlets.Unicode()
    location = traitlets.Unicode()
    num_workers = traitlets.Int()

    def __init__(self, name, num_workers, fwconfig=None, location='localhost',  wf_executor='fireworks'):

        super().__init__()

        self.futures = []
        self.workers = []
        self.name = name
        self.num_workers = num_workers
        self.wf_executor = wf_executor
        self.fwconfig = fwconfig
        self.location = location
        self.log_area = ipw.Output()

        if self.wf_executor == 'fireworks':
            self._add_workers(num_workers)

        if wf_executor == 'fireworks':
            self.init_fireworks()
        elif wf_executor == 'parsl':
            self.init_parsl()

    def _verify_executor(wf_executor):
        """Verify that given workflow executor is actually
        the one used by this WorkerPool.
        The syntax for decorators with arguments is strange.
        http://scottlobdell.me/2015/04/decorators-arguments-python/
        And for instance methods as decorators:
        https://stackoverflow.com/a/1263782/4228052
        """

        def decorator(func):
            def wrapper(self, *args, **kwargs):
                if self.wf_executor != wf_executor:
                    raise ValueError(
                    "Tried to use '{}' functionality, but current workflow executor for this WorkerPool is '{}'".format(wf_executor, self.wf_executor))

                else:
                    return func(self, *args, **kwargs)
            return wrapper
        return decorator

    def _add_workers(self, num_workers, *args, **kwargs):
        """Add workers to pool."""
        self.workers += [
            Worker(
                pool=self,
                wf_executor=self.wf_executor,
                *args,
                **kwargs
            )
            # TODO - might be more readable as a loop than list comprehension
            for i in range(num_workers)
        ]

    def _log_decorator(self, fun):
        """Execute function and log output."""
        #def wrapper(*args, **kwargs):
        #    with self.log_area:
        #        return fun(*args, **kwargs)
        # Turn off pool-level logging.
        # It's easier if it goes to the workflow widget.
        wrapper = fun
        return wrapper

    @_verify_executor('fireworks')
    def _fw_rapidfire(self, workflow):
        """Execute workflow in rapidfire with Workers."""

        # All workers should concurrently pull jobs.
        with cf.ThreadPoolExecutor() as executor:
            self.futures = []
            for i, worker in enumerate(self.workers):
                self.log_area.clear_output()
                self.futures.append(
                    executor.submit(
                        self._log_decorator(worker._worker_fw_rapidfire),
                        workflow=workflow
                    )
                )
                time.sleep(1)

    @_verify_executor('fireworks')
    def init_fireworks(self):
        """Create Fireworks LaunchPad for this pool."""

        # TODO - sanity checks on config file
        if self.fwconfig:
            with open(self.fwconfig) as param_file:
                params = yaml.load(param_file)
            self.lpad = fw.LaunchPad(**params)
        else:
            self.lpad = fw.LaunchPad()

        # TODO - resetting the FW DB here breaks if everything is not local
        #self.lpad.reset('', require_password=False)

    @_verify_executor('parsl')
    def init_parsl(self):
        """Create Parsl excecutors for this pool."""
        self.parsl_workers = cf.ThreadPoolExecutor(max_workers=self.num_workers)
        self.parsl_dfk = DataFlowKernel(executors=[self.parsl_workers])


    @_verify_executor('fireworks')
    def _fw_queue(self, workflow):
        """Generate subDAG and queue via Fireworks."""

        dag = workflow.gen_subdag()

        fw_tasks = []
        fw_links = {}

        for task in dag.nodes():
            print("Adding task {}".format(task.name))
            firework = task.get_firework(launch_dir=os.getcwd())
            fw_tasks.append(firework)
            child_list = []
            for child in dag.successors(task):
                child = child.get_firework(launch_dir=os.getcwd())
                child_list.append(child.fw_id)
                print("Adding link {} -> {}".format(task.name, child.name))
            fw_links[firework.fw_id] = child_list

        fw_workflow = fw.Workflow(fw_tasks, fw_links)
        self.lpad.add_wf(fw_workflow)

    @_verify_executor('fireworks')
    def fw_run(self, workflow):
        """Queue jobs from workflow and execute them all via Fireworks."""
        print("FW Run")
        self._fw_queue(workflow)
        print("FQ Queued")
        self._fw_rapidfire(workflow)
        print("FW Completed")

    @_verify_executor('parsl')
    def parsl_run(self, workflow):
        """Execute workflow via Parsl.
        So far, I'm assuming that we're only executing PythonFunctionTasks via Parsl.
        """
        print("parsl_run")

        # TODO: Remove workflow. from all of these.
        # They are only being saved for debugging.
        workflow.futures = dict()
        workflow.wrapped_funcs = dict()

        #print("nodes:")
        #print(list(networkx.dag.topological_sort(workflow.dag)))

        # Topological sort guarantees that parent node
        # appears in list before child.
        # Therefore, parent futures will exist
        # before children futures.
        for task in networkx.dag.topological_sort(workflow.dag):
            #print("Running {}".format(task.name))
            # Reset futures before submission
            task.reset_future()

            #print("""About to wrap.
            #name={},
            #task={},
            #args={},
            #kwargs={}.
            #future={}""".format(task.name, task, task.args, task.kwargs, task.future))
            workflow.wrapped_funcs[task] = parsl_wrap(
                task.func,
                self.parsl_dfk,
                *task.args,
                **task.kwargs
            )

            #print("wrapped funcs.")
            depends = [
                workflow.futures[dep]
                for dep in task.dependencies[workflow]
            ]
            #print("deps: {}".format([
            #    dep.name
            #    for dep in task.dependencies[workflow]
            #]))

            workflow.futures[task] = parsl_func_after_futures(
                workflow.wrapped_funcs[task],
                depends,
                self.parsl_dfk,
                *task.args,
                **task.kwargs
            )
            #print("future generated.")
            print()


class Worker(traitlets.HasTraits):
    """Computational resource on which to execute jobs.
    Should be created by WorkerPool.
    """

    pool = traitlets.Instance(WorkerPool)

    def __init__(self, pool, wf_executor='fireworks', *args, **kwargs):

        super().__init__()

        self.pool = pool

        if wf_executor == 'fireworks':
            self.fireworker = fw.FWorker(*args, **kwargs)

    def _worker_fw_rapidfire(self, workflow):
        return rapidfire(
            launchpad=self.pool.lpad,
            fworker=self.fireworker
        )


class Workflow(traitlets.HasTraits):

    dag = traitlets.Instance(networkx.DiGraph)
    name = traitlets.Unicode()
    index_dict = traitlets.Dict()
    fig_layout = traitlets.Instance(ipw.Layout)
    task_names = traitlets.List(trait=traitlets.Unicode())
    wf_executor = traitlets.Unicode(allow_none=True)
    readme = traitlets.Unicode()
    tag_dict = traitlets.Dict()

    def __init__(self, name):

        super().__init__()

        self.dag = networkx.DiGraph()
        self.name = name
        self.index_dict = {}
        #self.fig_layout = ipw.Layout(width='600px', height='800px')
        self.fig_layout = ipw.Layout(width='1000px', height='800px')
        self._task_names = []

        # Workflow executor - to be defined on initialization of wf executor.
        self.wf_executor = None

    def get_future(self, index):
        """Return future containing result from task with given index."""
        return self.index_dict[index].future

    def example_from_dag(self, dag):
        """
        Generate an example workflow from a DAG of numbers.
        Tasks must be numbered such that each task depends
        only on tasks with a smaller index.
        """
        tasks = [
                CommandLineTask(
                name='{num}',
                command='echo {num}',
                poll_interval=1,
                params={'num': i}
            )
            for i, node in enumerate(dag.nodes())
        ]

        for i in range(dag.number_of_nodes()):
            dependencies = [
                self.index_dict[index]
                for index in dag.predecessors(i)
            ]
            if len(dependencies) > 0:
                self.add_task(
                    tasks[i],
                    dependencies
                )
            else:
                self.add_task(tasks[i])

    def add_task(self, task, dependencies=None):
        """
        Add instantiated Task object to the Workflow.
        If dependencies=None, then this task will be executed
        as soon as possible upon starting the Workflow.
        A Task may appear only once per Workflow.
        """

        super().__init__()

        # Ensure that tasks are not repeated.
        if task in self.dag.nodes():
            raise ValueError("Task already present in Workflow. Please pass a deepcopy if you wish to repeat the Task.")
        elif task.name in self._task_names:
            raise ValueError("Task name '{}' already present in Workflow. Please use a unique name.".format(task.name))

        # Determine index for this Task in this Workflow
        index = self.dag.number_of_nodes()
        # Inform workflow and task of this assignment
        self.dag.add_node(task, index=index)
        task.index[self] = index
        self.index_dict[index] = task

        if dependencies is not None:
            # Store dependency relationship in DAG
            for dependency in dependencies:
                self.dag.add_edge(dependency, task)

            # Store dependency relationships in all involved nodes
            task.dependencies[self] = dependencies

            for dependency in dependencies:
                # Create child list for this workflow if not present
                if self not in dependency.children.keys():
                    dependency.children[self] = [task]
                else:
                    dependency.children[self].append(task)

        # Write empty list to dependency dict if none exist
        else:
            task.dependencies[self] = []

        # Tasks cannot have children at definition time.
        task.children[self] = []

        # Pass tag information to workflow
        self._task_add_tags(task, task.tags)

    def get_task_by_name(self, name):
        """Return the Task object with the given name in this Workflow."""
        for task in self.dag.nodes():
            try:
                if task.name == name:
                    return task
            except AttributeError:
                print("{} has no name.".format(task))

    ### Something will probably go wrong with tag_dict if a task belongs to multiple workflows ###

    def _task_add_tags(self, task, tags):
        """Add list of tags to task."""
        task.tags = list(set().union(task.tags, tags))
        for tag in tags:
            if tag not in self.tag_dict:
                self.tag_dict[tag] = [task]
            elif task not in self.tag_dict[tag]:
                self.tag_dict[tag].append(task)

    def _task_remove_tags(self, task, tags):
        """Remove list of tags from task"""
        task.tags = list(set(task.tags).difference(tags))
        for tag in tags:
            index = self.tag_dict[tag].index(task)
            self.tag_dict[tag].pop(index)

    def _del_tag(self, tag):
        """Delete tag and remove all """
        tasks = self.tag_dict[tag]
        for task in tasks:
            self._task_remove_tags(task, [tag])
        del self.tag_dict[tag]

    def _new_tag(self, tag):
        if tag not in self.tag_dict.keys():
            self.tag_dict[tag] = []

    def _tag_set_tasks(self, tag, tasks):
        """Add tag to given tasks, and remove it from others."""
        # Remove tag from all other tasks
        presently_tagged = self.tag_dict[tag][:]
        for task in presently_tagged:
            if task not in tasks:
                self._task_remove_tags(task, [tag])
        # Add tag to these tasks
        for task in tasks:
            self._task_add_tags(task, [tag])

    def _gen_bqgraph(self):
        """Generate bqplot graph."""

        pos = networkx.nx_pydot.graphviz_layout(self.dag, prog='dot')
        N = self.dag.number_of_nodes()

        x, y = [[pos[node][i] for node in self.dag.nodes()] for i in range(2)]

        # TODO: Some items of interest to be displayed in WorkflowWidget
        # are not serializable (e.g. a future as a PythonFunctionTask
        # argument). It may be necessary to have get_user_dict return
        # representations rather than the actual objects.
        node_data = [
            {
                'label': str(node.index[self]),
                'shape': 'rect',
                **node.get_user_dict()
            }
            for node in self.dag.nodes()
        ]
        link_data = [
            {
                'source': source.index[self],
                'target': target.index[self]
            }
            for source, target in self.dag.edges()
        ]

        xs = bq.LinearScale()
        ys = bq.LinearScale()
        scales = {'x': xs, 'y': ys}


        graph = bq.Graph(
            node_data=node_data,
            link_data=link_data,
            scales=scales,
            link_type='line',
            highlight_links=False,
            x=x, y=y,
            selected_style={'stroke':'red'}
            #interactions = {
            #    'click': 'tooltip',
            #    'hover': 'select'
            #},
        )

        # graph.tooltip = bq.Tooltip(
        #     fields=self.dag.nodes()[0].user_fields
        # )

        return graph

    def get_bqgraph(self):
        """Retrieve, but do not regenerate bqplot graph."""
        return self._bqgraph

    def draw_dag(self, layout=None):
        """Return bqplot figure representing DAG, regenerating graph."""

        self._bqgraph = self._gen_bqgraph()

        graph = self.get_bqgraph()
        if layout == None:
            layout = self.fig_layout

        fig = bq.Figure(marks=[graph], layout=layout)

        toolbar = bq.Toolbar(figure=fig)

        return ipw.VBox([fig, toolbar])

    def check_selection_for_dependency_gaps(self):
        """
        It's okay for a selcted task to have a parent or child
        which is not selected. But it's not okay to have
        a generational gap in selection.

        TODO: Check for gaps in workflow.
        Ultimately, the workflow should not run and the user
        should be notified if an incorrect selection is made.

        Perhaps by raising & catching exception, though
        it may be better just to use an if statement.
        """
        print("TODO: Not checking for subDAG gaps")
        pass

    def gen_subdag(self):
        """Return DAG containing only steps which are to be run based on bqplot selection.
        Currently, there may not be any gaps in dependencies.
        e.g. if A -> B -> C, then A and C cannot be selected without B.
        """
        subdag = networkx.DiGraph()

        # If nothing is selected, then use the full workflow
        if self._bqgraph.selected is None or len(self._bqgraph.selected) == 0:
            return self.dag

        # If only some tasks are selected, then check for gaps
        # and generate the subDAG
        else:
            self.check_selection_for_dependency_gaps()

            # Add each node in selection
            for node_index in self._bqgraph.selected:
                # Identify task by index
                node = self.index_dict[node_index]
                # Identify parent in main DAG
                parent_list = list(self.dag.predecessors(node))
                for parent in parent_list:
                    parent_index = parent.index[self]
                    # Check whether parent is selected
                    if parent_index in self._bqgraph.selected:
                        # Creating the edge creates both
                        # nodes if they aren't in the graph,
                        # but avoids creating duplicate nodes.
                        subdag.add_edge(parent, node)

            return subdag

    def export_cwl(self, cwl_file):
        pass


class Task(traitlets.HasTraits):
    """One step in a Workflow. Must have a unique name."""

    name = traitlets.Unicode()
    task_type = traitlets.Unicode()
    readme = traitlets.Unicode()
    input_files = traitlets.List(trait=traitlets.Unicode())
    output_files = traitlets.List(trait=traitlets.Unicode())
    log_path = traitlets.Unicode()
    num_cores = traitlets.Int()
    index = traitlets.Dict()
    dependencies = traitlets.Dict()
    children = traitlets.Dict()
    params = traitlets.Dict()
    tags = traitlets.List()
    notebook = traitlets.Unicode(allow_none=True)

    # TODO - set all defaults to None, check each arg and assign list as needed
    def __init__(self, name, input_files=[], output_files=[], log_path='',
                 params={}, num_cores=1, task_type='', readme='', tags=[],
                 notebook=None,
                 substitute_strings=[], substitute_lists=[],
                 user_fields=[]):

        super().__init__()

        # Name of task (must be unique)
        self.name = name

        # Type of task (Notebook, CommandLine, etc.)
        self.task_type = task_type

        # HTML string (hopefully soon markdown) explaining this task
        self.readme = readme

        # Tags - used for selecting & running groups of tasks.
        self.tags = tags

        # For interactive monitoring (or standalone for NotebookTasks)
        self.notebook = notebook

        # Files which this Task takes as input
        # and must be present before run.
        self.input_files = input_files

        # Files which are generated or modified by this Task.
        self.output_files = output_files

        # Path to log file for this Task
        self.log_path = log_path

        # Number of CPU cores to run the task on
        self.num_cores = num_cores

        # Map workflow to the node index which
        # represents this task in that workflow.
        # Tasks may be in multiple workflows,
        self.index = {}

        # List of other Tasks which must complete
        # before this Task can be run.
        self.dependencies = {}

        # List of Tasks which depend on this Task.
        self.children = {}

        # Parameters to replace in other arguments
        self.params = params

        # List of names of fields to substitute params.
        # If a child class calls Task.__init__ with
        # substitute_strings or substitute_lists as
        # nonempty lists, they will be included here.
        self._substitute_strings = [
            'name',
            'task_type',
            'log_path',
            'readme'
        ] + substitute_strings
        self._substitute_lists = [
            'input_files',
            'output_files',
            'tags'
        ] + substitute_lists

        self._substitute_fields()

        # Fields which are of interest to the user
        self.user_fields = [
            'name',
            'task_type',
            'tags',
            'input_files',
            'output_files',
            'num_cores'
        ] + user_fields

        # Initialize None as placeholder for Firework executable.
        self._firework = None

    def get_user_dict(self):
        """Generate dictionary of user field names and values"""
        return {
            field: getattr(self, field)
            for field in self.user_fields
        }

    def get_firework(self, launch_dir=None):
        """Return Firework if it exists, and create it otherwise.
        Default to creating new directory for output (FW default).
        Pass launch_dir='.' to run in current directory.
        """

        # Only create new firework if it doesn't already exist.
        if self._firework is None:
            if launch_dir is None:
                spec = None
            else:
                # Turn relative paths into absolute paths to pass to fireworks.
                launch_dir = os.path.abspath(launch_dir)

                spec = {'_launch_dir': launch_dir}

            # Create firework
            self._firework = fw.Firework(
                self._gen_firetask(),
                name=self.name,
                spec=spec
            )

        return self._firework

    # TODO - fill this out
    def _gen_firetask(self):
        pass

    def _substitute_fields(self):
        """Replace fields according to params dict."""
        for field in self._substitute_strings:
            # Read current value
            before = getattr(self,field)
            # Replace fields
            after = before.format(**self.params)
            # Write new value
            setattr(self, field, after)

        for list_name in self._substitute_lists:
            field_list = getattr(self, list_name)
            # Read current values
            for i, before in enumerate(field_list):
                # Replace fields
                after = before.format(**self.params)
                # Write to working copy of list
                field_list[i] = after
            # Write working copy to actual list
            setattr(self, list_name, field_list)

    def _run(self):
        """
        Run this Task. Should be executed by a Workflow.
        This function should be overloaded by child classes.
        """
        print("Task run.")


class NotebookTask(Task):
    """

    Jupyter Notebook which should appear as a node in the Workflow DAG.
    If interactive == True, a kernel will be started and the
    notebook will be opened for user to interact with.
    Workflow will be blocked in the meantime.
    If false, notebook will be executed without opening,
    and Workflow will continue upon successful execution.
    """

    randhash = traitlets.Unicode(allow_none=True)

    def __init__(self, name, interactive=True, **kwargs):
        self.task_type = 'NotebookTask'
        self.interactive = interactive
        user_fields = ['interactive']

        self.randhash = None
        self.success_file = None

        super().__init__(
            name=name,
            user_fields=user_fields,
            **kwargs)

    def _continue_workflow(self):
        with open(self.success_file, 'w') as fh:
            fh.write(self.randhash)

    def _run(self):
        print("Notebook run.")

    def _gen_firetask(self):
        self.randhash = batch_jobs.gen_random_hash()
        self.success_file = batch_jobs.create_success_file()

        return fw.PyTask(
            func='kale.nb_task.nb_poll_success_file',
            kwargs=dict(
                randhash=self.randhash,
                success_file=self.success_file,
                poll_interval=1
            )
        )

class CommandLineTask(Task):
    """Command Line Task to be executed as a Workflow step."""
    def __init__(self, name, command, nodes_cores=1, node_property=None, poll_interval=60, **kwargs):

        self.command = command
        self.node_property = node_property
        self.nodes_cores = nodes_cores
        self.poll_interval = poll_interval

        user_fields = ['command']

        super().__init__(
            name=name,
            task_type='CommandLineTask',
            substitute_strings=['command'],
            user_fields=user_fields,
            **kwargs
        )

    def _run(self):
        print("Command Line run.")

    def _gen_firetask(self):
        """Create a Firework for this task."""
        #return fw.ScriptTask.from_str(self.command)
        return fw.PyTask(
            func='kale.batch_jobs.run_cmd_job',
            kwargs=dict(
                command=self.command,
                name=self.name,
                nodes_cores=self.nodes_cores,
                node_property=self.node_property,
                poll_interval=self.poll_interval
            )
        )


class PythonFunctionTask(Task):
    """Python function call to be executed as a Workflow step."""
    def __init__(self, name, func, args=[], kwargs={}, **other_kwargs):
        # Actual callable function to be executed.
        #self.func = func
        self.func = self.wrap_func(func)
        self.args = args
        self.kwargs = kwargs
        self.future = cf.Future()

        user_fields = ['args', 'kwargs']

        super().__init__(
            name=name,
            task_type='PythonFunctionTask',
            #user_fields=user_fields,
            **other_kwargs
        )

    def _run(self):
        print("Python function run.")
        return self.func(*self.args, **self.kwargs)

    def _gen_firetask(self):
        return fw.PyTask(
            func=self.func.__name__,
            args=self.args,
            kwargs=self.kwargs
        )

    def reset_future(self):
        """Replace self.future with a new Future.
        This should be called upon workflow submission."""
        #print("future before: {}".format(self.future))
        self.future = cf.Future()
        #print("future after: {} \n now = {}".format(self.future, datetime.now()))

    def parse_args(self, args, kwargs):
        """Replace any Task in args or kwargs with the result of their future.
        This should be called immediately before execution."""
        import time

        #print("""Parsing args: {}
        #now = {}
        #task = {}""".format(args, datetime.now(), self))
        new_args = []
        for arg in args:
            if isinstance(arg, Task):
                #print("before result.")
                new_args.append(arg.future.result())
                #print("after result.")
            else:
                new_args.append(arg)

        #print("Parsing kwargs: {}".format(kwargs))
        new_kwargs = dict()
        for key, val in kwargs.items():
            if isinstance(val, Task):
                new_kwargs[key] = val.future.result()
            else:
                new_kwargs[key] = val

        return new_args, new_kwargs

    def wrap_func(self, func):
        """Wrap the function so that any futures in `args` or `kwargs`
        are replaced by their results,
        AND that the return value of this function will be
        accessible via PythonFunctionTask.future.result().

        NOTE: This will almost certainly break Fireworks execution.
        because it relies on func.__name__. Not 100% sure.
        """
        def wrapper(*args, **kwargs):
            import time
            time.sleep(0.5)
            #print("\nWaiting...")
            #print("Wrapper running.")
            #print("Given args={}, kwargs={}".format(args, kwargs))
            new_args, new_kwargs = self.parse_args(args, kwargs)
            #print("Executing w/ new_args={}, new_kwargs={}".format(new_args, new_kwargs))
            #print("Args parsed.")
            # This is where function execution occurs.
            result = func(*new_args, **new_kwargs)
            #print("The result is {}".format(result))
            self.future.set_result(result)
            #print("Result set.")

        return wrapper

class BatchTask(Task):
    """Task which will be submitted to a batch queue to execute."""
    def __init__(self, name, batch_script, node_property=None, poll_interval=60, **kwargs):
        self.batch_script = batch_script
        self.node_property = node_property
        self.poll_interval = poll_interval

        user_fields = ['batch_script']
        substitute_strings = ['batch_script']

        super().__init__(
            name=name,
            task_type='BatchTask',
            user_fields=user_fields,
            substitute_strings=substitute_strings,
            **kwargs
        )

    def _gen_firetask(self):
        return fw.PyTask(
            func='kale.batch_jobs.run_batch_job',
            args=[self.batch_script],
            kwargs=dict(
                # TODO - review node_property
                #node_property=self.node_property,
                poll_interval=self.poll_interval
            )
        )

    def _run(self):
        print("Batch run.")
