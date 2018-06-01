# 3rd party
import drmaa
import networkx

class DRMAAJob(object):
    def __init__(self, command, args):
        self._jobid = None
        self._command = command
        self._args = args

    def submit(self, join_files=True):
        with drmaa.Session() as s:
            jt = s.createJobTemplate()
            jt.remoteCommand = self._command
            jt.args = list(self._args)
            jt.joinFiles = join_files
            self._jobid = s.runJob(jt)
            s.deleteJobTemplate(jt)

    def status(self):
        with drmaa.Session() as s:
            status = s.jobStatus(self._jobid)
        return status

    def _control(self, command):
        with drmaa.Session() as s:
            s.control(self._jobid, command)

    def suspend(self):
        self._control(drmaa.JobControlAction.SUSPEND)

    def resume(self):
        self._control(drmaa.JobControlAction.RESUME)

    def terminate(self):
        self._control(drmaa.JobControlAction.TERMINATE)

    def wait(self, timeout=None):
        if timeout is None:
            _t = drmaa.Session.TIMEOUT_WAIT_FOREVER
        else:
            assert isinstance(timeout, int), "timeout must be an integer, received {}".format(timeout)
            _t = timeout

        with drmaa.Session() as s:
            jobinfo = s.wait(self._jobid, _t)

        return jobinfo


class WorkflowJob(DRMAAJob):
    def __init__(self, workflow):
        super().__init__(command="run_workflow", args=workflow)
        self._wf = workflow

    def run(self):
        graph = self._wf._graph
        remaining = [x for x in graph.nodes if graph.in_degree(x) == 0]

        while len(remaining) > 0:
            n = remaining.pop()

            for t in n._tasks:
                jobs.run_function(t)

            if graph.out_degree(n) > 0:
                remaining.extend([x for x in networkx.bfs_successors(graph, n)])


class WorkflowTaskJob(DRMAAJob):
    def __init__(self, task):
        super().__init__(command="run_task",  args=task)
        self._task = task
