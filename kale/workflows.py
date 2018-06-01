# stdlib
import random
import abc

# 3rd party
import bqplot
import ipywidgets
import networkx


class Workflow(object):
    """A Workflow is a directed graph containing at least one WorkflowNode and zero or more edges."""
    def __init__(self):
        self._graph = networkx.DiGraph()

    def add_node(self, node):
        assert isinstance(node, WorkflowNode), "A node must be a valid WorkflowNode!"
        self._graph.add_node(node)

    def remove_node(self, id):
        self._graph.remove_node(id)

    def nodes(self):
        return self._graph.nodes()

    def add_edge(self, from_node, to_node):
        self._graph.add_edge(from_node, to_node)

    def remove_edge(self, from_node, to_node):
        self._graph.remove_edge(from_node, to_node)

    def edges(self):
        return self._graph.edges()

    def display(self):
        pos = networkx.nx_pydot.graphviz_layout(self._graph, prog='dot')
        x, y = zip(*(pos[node] for node in self._graph.nodes()))

        node_data = [
            {
                'label': str(node._id),
                'shape': 'rect',
                'tasks': [str(t) for t in node._tasks]
                }
            for node in self._graph.nodes()
            ]

        link_data = [
            {
                'source': source.index[self],
                'target': target.index[self]
                }
            for source, target in self._graph.edges()
            ]

        xs = bqplot.LinearScale()
        ys = bqplot.LinearScale()
        scales = {'x': xs, 'y': ys}

        graph = bqplot.Graph(
            node_data=node_data,
            link_data=link_data,
            scales=scales,
            colors=['white'],
            link_type='line',
            highlight_links=False,
            x=x, y=y,
            selected_style={
                'stroke': 'yellow',
                'stroke-width': '4',
                'opacity': '0.5'}
            )

        tooltip = bqplot.Tooltip(fields=['tasks'])
        graph.tooltip = tooltip

        layout = ipywidgets.Layout(
            min_width='50%',
            width='auto',
            min_height='200px',
            height='auto')
        fig = bqplot.Figure(marks=[graph], layout=layout)
        return ipywidgets.VBox([fig])
        #toolbar = bqplot.Toolbar(figure=fig)
        #return ipywidgets.VBox([fig, toolbar])


class WorkflowNode(object):
    """A WorkflowNode contains one or more WorkflowTasks."""
    def __init__(self, id=None):
        self._task_ids = {}
        self._tasks = []
        self._id = id or random.randint(1,2**64)

    def list_tasks(self):
        return (t.__repr__() for t in self._tasks)

    def add_task(self, task):
        assert isinstance(task, WorkflowTask), "The task must be a valid WorkflowTask!"
        if task._id in self._task_ids:
            raise ValueError("WorkflowTask {} already exists in this WorkflowNode.".format(task._id) +
                             " Each WorkflowTask must have a unique id.")

        self._task_ids[task._id] = len(self._tasks)
        self._tasks.append(task)

    def remove_task(self, id):
        assert id is not None, "A valid WorkflowTask id is required!"
        if id not in self._task_ids:
            raise LookupError("Unable to find WorkflowTask {}!".format(id))

        del self._tasks[self._task_ids[id]]
        self._task_ids.pop(id)


class WorkflowTask(object):
    """A WorkflowTask represents a discrete computational step."""
    def __init__(self, label=None):
        self._label = label or "WorkflowTask"
        # assign unique string id
        self._id = "{}_{}".format(self._label, random.randint(1,32768))

    @abc.abstractmethod
    def to_file(self):
        pass

    @abc.abstractclassmethod
    def from_file(self, f):
        pass

    @abc.abstractmethod
    def __repr__(self):
        pass


class FunctionTask(WorkflowTask):
    def __init__(self, func, **kwargs):
        super().__init__()

        self.func = func

    def to_file(self):
        pass

    def from_file(self, f):
        pass

    def __repr__(self):
        return self.func


class CommandLineTask(WorkflowTask):
    def __init__(self, command, **kwargs):
        super().__init__()

        self.command = command

    def to_file(self):
        pass

    def from_file(self, f):
        pass

    def __repr__(self):
        return self.command


class WorkflowTaskExecutor(object):
    def __init__(self, task):
        self._task = task

    def start(self):
        pass

    def stop(self):
        pass

    def suspend(self):
        pass

    def resume(self):
        pass

    def wait(self):
        pass



class FireworksWorkflow(Workflow):
    def __init__(self, fw_workflow=None):
        super().__init__()
        self._fw_workflow = fw_workflow

        if self._fw_workflow is not None:
            for fw in fw_workflow.fws:
                self._graph.add_node(fw)
                if fw.fw_id in fw_workflow.links:
                    for link in fw_workflow.links[fw.fw_id]:
                        self._graph.add_edge(fw.fw_id, link)

    def display(self):
        pos = networkx.nx_pydot.graphviz_layout(self._graph, prog='dot')
        x, y = zip(*(pos[node] for node in self._graph.nodes()))

        fw_id_to_node_index = {self._fw_workflow.fws[i].fw_id: i for i in range(len(self._fw_workflow.fws))}

        node_data = [
            {
                'label': str(node.fw_id),
                'name': node.name,
                'state': node.state,
                'tasks': [str(t) for t in node.tasks],
                'shape': 'rect',
                }
            for node in self._fw_workflow.fws
            ]

        link_data = [
            {
                'source': fw_id_to_node_index[source],
                'target': fw_id_to_node_index[target]
                }
            for source, target in self._graph.edges()
            ]

        xs = bqplot.LinearScale(min=min(x),max=max(x))
        ys = bqplot.LinearScale(min=min(y),max=max(y))
        scales = {'x': xs, 'y': ys}

        graph = bqplot.Graph(
            node_data=node_data,
            link_data=link_data,
            scales=scales,
            colors=['white'],
            link_type='line',
            highlight_links=False,
            x=x, y=y,
            selected_style={
                'stroke': 'yellow',
                'stroke-width': '4',
                'opacity': '0.5'}
            )

        tooltip = bqplot.Tooltip(fields=['name', 'state', 'tasks'], format=['','',''])
        graph.tooltip = tooltip

        layout = ipywidgets.Layout(
            min_width='50%',
            width='auto',
            min_height='200px',
            height='auto')
        fig = bqplot.Figure(marks=[graph], layout=layout)
        #toolbar = bqplot.Toolbar(figure=fig)
        #return ipywidgets.VBox([fig, toolbar])
        return ipywidgets.VBox([fig])
