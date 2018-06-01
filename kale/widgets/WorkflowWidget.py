# stdlib
from concurrent.futures import ThreadPoolExecutor

# 3rd party
import bqplot as bq
import ipywidgets as ipw
import traitlets

# local
import kale.workflow_objects
from . import utils


class EditHTML(ipw.VBox):
    def __init__(self, value='', text_height=400):
        super().__init__()
        self.HTML = ipw.HTMLMath(value=value)
        self.Text = ipw.Textarea(value=value)
        self.toggle_button = ipw.Button()

        self.elements = [self.HTML, self.Text]
        self.descriptions = ['Edit Description', 'Render Description']
        traitlets.link((self.HTML, 'value'), (self.Text, 'value'))

        # Set height and width of Textarea
        self.Text.layout.height = u'{}px'.format(text_height)
        self.Text.layout.width = u'95%'
        self.HTML.layout.border=u'1px lightgray solid'
        self.HTML.layout.padding=u'10px'

        # Set HTML view by default
        # 0 = HTML view
        # 1 = Edit view
        self.set_view(0)

        self.toggle_button.on_click(self.toggle)

    def set_view(self, state):
        self.state = state
        self.children = [self.elements[state], self.toggle_button]
        self.toggle_button.description = self.descriptions[state]

    # TODO - caller unused
    def toggle(self, caller):
        self.set_view((self.state+1)%2)


# TODO - review
class WorkflowWidget(ipw.HBox):
    """Widget to draw DAG via bqplot and provide node-level info/interaction."""

    displayed_task = traitlets.Any()

    @property
    def bqgraph(self):
        return self.workflow.get_bqgraph()

    def __init__(self, workflow, worker_pool_widget):
        super().__init__()

        # Define variables
        self.workflow = workflow
        self.worker_pool_widget = worker_pool_widget
        self._fig_layout = ipw.Layout(width='400px', height='600px')
        self._xs = bq.LinearScale()
        self._ys = bq.LinearScale()
        self._scales = {'x': self._xs, 'y': self._ys}
        mgin = 10

        self.displayed_task = None
        # Link which binds readme text of selected task to displayed value
        self._task_readme_link = None

        # Define elements
        self._metadata_template = """
        Node name: {name}
        <br>
        Last modified: {date}
        <br>
        Description: {word}
        """
        self._metadata_html = ipw.HTML()

        self._workflow_readme_html = EditHTML()
        self._task_readme_html = EditHTML()

        self._launch_notebook_link = ipw.HTML()
        self._continue_workflow_button = ipw.Button(
            description='Continue Workflow',
            button_style='info',
            disabled=False,
            layout={'visibility': 'hidden'}
        )

        self._continue_workflow_button.on_click(
            self._continue_workflow
        )

        self._worker_pool_selector = ipw.Dropdown()
        self._run_button = ipw.Button(
            description='Run Workflow',
            button_style='success'
        )

        self._workflow_controls = ipw.VBox([
            self._worker_pool_selector,
            self._run_button
        ])
        self._log_path_input = ipw.Text(
            description='Log path',
            value='',
            disabled=True
        )
        self._log_html = ipw.HTML()

        # self._info_area = ipw.VBox([
        #     self._readme_html,
        #     utils.Space(height=20),
        #     self._metadata_html,
        #     self._notebook_button
        # ])

        self._workflow_area = ipw.VBox([
            ipw.HTML("<b>Workflow Description</b>"),
            self._workflow_readme_html,
            utils.Space(height=20),
            ipw.HTML("<b>Worker Pools</b>"),
            self._workflow_controls
        ])

        self._task_area = ipw.VBox([
            ipw.HTML("<b>Task Description</b>"),
            self._task_readme_html,
            utils.Space(height=20),
            self._launch_notebook_link,
            self._continue_workflow_button,
            utils.Space(height=20),
            ipw.HTML("<b>Task Metadata</b>"),
            self._metadata_html

        ])

        self._tb_select_all = ipw.Button(
            description = 'Select All',
        )
        self._tb_select_none = ipw.Button(
            description = 'Select None',
        )
        self._tb_select_children = ipw.Button(
            description = 'Select Children',
        )
        self._tb_select_parents = ipw.Button(
            description = 'Select Parents',
        )
        self._tb_tag_to_selection = ipw.Button(
            description = 'Tag to Selection',
        )
        self._tb_selection_to_tag = ipw.Button(
            description = 'Selection to Tag',
        )

        self._tb_new_tag = ipw.Button(
            description = 'New Tag'
        )
        self._tb_del_tag = ipw.Button(
            description = 'Del Tag'
        )

        self._tb_select_all.on_click(self._ta_select_all)
        self._tb_select_none.on_click(self._ta_select_none)
        self._tb_select_children.on_click(self._ta_append_all_children)
        self._tb_select_parents.on_click(self._ta_append_all_parents)
        self._tb_selection_to_tag.on_click(self._ta_selection_to_tag)
        self._tb_tag_to_selection.on_click(self._ta_tag_to_selection)

        self._tb_new_tag.on_click(self._ta_new_tag)
        self._tb_del_tag.on_click(self._ta_del_tag)


        self._tag_selector = ipw.Select()

        self._new_tag_name = ipw.Text(
            description='Tag name'
        )

        self._tag_buttons = ipw.HBox([
            ipw.VBox([
                self._tb_select_all,
                self._tb_select_none,
                self._tb_select_children,
                self._tb_select_parents,
                self._tb_tag_to_selection,
                self._tb_selection_to_tag
            ]),
            ipw.VBox([
                self._tb_new_tag,
                self._tb_del_tag
            ])
        ])

        self._tag_area = ipw.VBox([
            ipw.HTML("<b>Tags</b>"),
            self._tag_selector,
            ipw.HTML("<b>Actions</b>"),
            self._new_tag_name,
            self._tag_buttons,
        ])

        # Log messages produced by WorkflowWidget
        self._widget_log = ipw.Output()
        self._log_clear_button = ipw.Button(
            description="Clear"
        )
        self._widget_log_container = ipw.Box(
            [self._widget_log],
            layout=ipw.Layout(
                padding='10px',
                border='1px lightgray solid'
            )
        )
        self._widget_log_area = ipw.VBox([
            ipw.HTML("<b>Messages from WorkflowWidget:</b>"),
            self._widget_log_container,
            utils.Space(10),
            self._log_clear_button
        ])


        # View for log file produced by workflow
        self._log_area = ipw.VBox([
            self._log_path_input,
            self._log_html
        ])

        self._graph_container = workflow.draw_dag(layout=self._fig_layout)
        self._graph_figure = self._graph_container.children[0]

        self._tab = ipw.Tab([
            #self._readme_area,
            self._workflow_area,
            self._task_area,
            self._tag_area,
            self._widget_log_area
            #self._info_area,
            #self._log_area
        ])

        self.output_area = ipw.Output()

        # Define layout
        self.children = [
            self._graph_container,
            self._tab,
        ]

        # Set attributes
        #self._tab.set_title(0, 'Readme')
        self._tab.set_title(0, 'Workflow')
        self._tab.set_title(1, 'Task')
        self._tab.set_title(2, 'Tags')
        self._tab.set_title(3, 'Widget Log')
        self._tab.layout.height = self._fig_layout.height
        self._tab.layout.width = self._fig_layout.width

        self._graph_figure.fig_margin = dict(
            left=mgin,
            right=mgin,
            bottom=mgin,
            top=mgin
        )
        self._graph_figure.min_aspect_ratio = 0

        # Graph style
        self.bqgraph.selected_style = dict(
            stroke='red'
        )

        # Default selections
        self._tab.selected_index = 0
        self.bqgraph.selected = [0]
        self.nodes = list(self.workflow.dag.nodes())

        self._thread_pool = ThreadPoolExecutor()
        self.future = None

        # TODO - onsubmit deprecated
        # Logic
        self.bqgraph.observe(self._call_update_selected_node, names='selected')
        self._log_path_input.on_submit(self._call_read_log)
        self._log_clear_button.on_click(self._clear_widget_log)

        # Inform worker_pool_widget of association
        self.worker_pool_widget._workflow_widgets += [self]
        # Manually update list.
        self.worker_pool_widget._update_worker_pool_list()

        self._run_button.on_click(self._run_wrapper)

        # Update tag selector now and when tag_list changes
        self._update_tag_selector()
        # This observe doesn't seem to work. Maybe better to just
        # have selector updated every time tag_dict is changed.
        #self.workflow.observe(self._update_tag_selector, names='tag_dict')


        # Link workflow readme
        self._workflow_readme_html.HTML.value = self.workflow.readme
        self._workflow_readme_link = traitlets.link(
            (self._workflow_readme_html.HTML, 'value'),
            (self.workflow, 'readme')
        )

        # Run updates
        self._update_readme_html()
        self._update_log()

    def _continue_workflow(self, *args):
        self.displayed_task._continue_workflow()

    def _update_tag_selector(self, change=None):
        """Update tag selector to match tag_dict"""
        self._tag_selector.options = self.workflow.tag_dict.keys()

    def get_selected_tasks(self):
        selected = self.bqgraph.selected
        names = [self.bqgraph.node_data[task]['name'] for task in selected]
        selected_tasks = [self.workflow.get_task_by_name(name) for name in names]
        return selected_tasks

    def get_selected_task_indices(self):
        return [task.index[self.workflow] for task in self.get_selected_tasks()]

    def _get_children(self, tasks):
        """Return children of all provided tasks."""
        children_list = []
        for task in tasks:
            children = self.workflow.dag.successors(task)
            children_list.append(children)

        all_children = list(set().union(*children_list))
        return all_children

    def _get_parents(self, tasks):
        """Return parents of provided tasks"""
        parents_list = []
        for task in tasks:
            parents = self.workflow.dag.predecessors(task)
            parents_list.append(parents)

        all_parents = list(set().union(*parents_list))
        return all_parents

    # Tag actions

    def _ta_select_all(self, *args, **kwargs):
        num_nodes = self.workflow.dag.number_of_nodes()
        node_list = list(range(num_nodes))
        self.bqgraph.selected = node_list

    def _ta_select_none(self, *args, **kwargs):
        self.bqgraph.selected = None

    def _ta_select_children(self, *args, **kwargs):
        """Select children of all presently selected tasks."""
        selected_tasks = self.get_selected_tasks()
        all_children = self._get_children(selected_tasks)
        children_indices = [child.index[self.workflow] for child in all_children]
        if len(children_indices) > 0:
            self.bqgraph.selected = children_indices
        else:
            self.bqgraph.selected = None

    def _ta_select_parents(self, *args, **kwargs):
        """Append parents of all presently selected tasks to the selection."""
        selected_tasks = self.get_selected_tasks()
        all_parents = self._get_parents(selected_tasks)
        parents_indices = [parent.index[self.workflow] for parent in all_parents]
        if len(parents_indices) > 0:
            self.bqgraph.selected = parents_indices
        else:
            self.bqgraph.selected = None

    def _ta_append_all_children(self, *args, **kwargs):
        """Recursively append children's children to selection."""
        selected = self.get_selected_tasks()
        all_children = selected
        direct_children = selected
        while len(direct_children) > 0:
            direct_children = self._get_children(direct_children)
            names = [child.name for child in direct_children]
            all_children += direct_children
        indices = [child.index[self.workflow] for child in all_children]
        self.bqgraph.selected = indices

    def _ta_append_all_parents(self, *args, **kwargs):
        """Recursively append parents's parents to selection."""
        selected = self.get_selected_tasks()
        all_parents = selected
        direct_parents = selected
        while len(direct_parents) > 0:
            direct_parents = self._get_parents(direct_parents)
            names = [parent.name for parent in direct_parents]
            all_parents += direct_parents
        indices = [parent.index[self.workflow] for parent in all_parents]
        self.bqgraph.selected = indices

    def _ta_tag_to_selection(self, *args, **kwargs):
        tag = self._tag_selector.value
        tagged_tasks = self.workflow.tag_dict[tag]
        tagged_task_indices = [task.index[self.workflow] for task in tagged_tasks]
        if len(tagged_task_indices) > 0:
            self.bqgraph.selected = tagged_task_indices
        else:
            self.bqgraph.selected = None

    def _ta_selection_to_tag(self, *args, **kwargs):
        tag = self._tag_selector.value
        selected_indices = self.bqgraph.selected
        selected_tasks = [self.workflow.index_dict[index] for index in selected_indices]
        self.workflow._tag_set_tasks(tag, selected_tasks)

    def _ta_new_tag(self, *args, **kwargs):
        tag = self._new_tag_name.value
        self._new_tag_name.value = ''
        self.workflow._new_tag(tag)
        self._update_tag_selector()

    def _ta_del_tag(self, *args, **kwargs):
        tag = self._tag_selector.value
        self.workflow._del_tag(tag)
        self._update_tag_selector()

    def _clear_widget_log(self, *args, **kwargs):
        self._widget_log.clear_output()

    def _update_log(self, task=None):
        if task is None:
            self._log_path_input.value = ''
            self._log_html.value = ''
        else:
            self._log_path_input.value = task.log_path
            self._call_read_log()

    def _update_metadata_html(self, metadata):
        html = "<br>".join([
            """
            <b>{key}:</b> {value}
            """.format(
                key=key,
                value=value
                )
            for key,value in metadata.items()
        ])

        with self.output_area:
            print(html)

        self._metadata_html.value = html

    def _update_readme_html(self, task=None):
        """Link README HTML to task."""

        # if task is None:
        #     name = 'None'
        #     readme = 'None'
        # else:
        #     name = task.name
        #     readme = task.readme
        # print("Updating to task '{}'".format(name))
        # print("Readme: '{}'".format(readme))

        # Break previous link if it exists
        try:
            self._task_readme_link.unlink()
            self._task_readme_link = None
        except AttributeError:
            pass

        # Only create new link if a task is selected
        if task is None:
            self._task_readme_html.HTML.value = "None selected."

            # Disable editing if nothing is selected.
            self._task_readme_html.toggle_button.disabled = True

            # Switch to HTML view if currently in edit mode.
            self._task_readme_html.set_view(0)
        else:
            # Update displayed HTML before linking
            self._task_readme_html.HTML.value = task.readme

            # Link values
            self._task_readme_link = traitlets.link(
                (self._task_readme_html.HTML, 'value'),
                (task, 'readme')
            )

            self._task_readme_html.toggle_button.disabled = False

            # "Continue Workflow" button
            if type(task) == kale.workflow_objects.NotebookTask:
                self._show_continue_workflow_button()
            else:
                self._hide_continue_workflow_button()

    def update_launch_notebook_link(self, *args):
        task = self.displayed_task
        if task is None or task.notebook is None:
            self._launch_notebook_link.value = ''
        else:
            self._launch_notebook_link.value = """
                <a href='{}' target='_blank'>Launch Notebook</a>
                """.format(task.notebook)

    def _show_continue_workflow_button(self, *args):
        self._continue_workflow_button.layout.visibility = 'visible'
    def _hide_continue_workflow_button(self, *args):
        self._continue_workflow_button.layout.visibility = 'hidden'
    def _enable_continue_workflow_button(self, *args):
        self._continue_workflow_button.disabled = False
    def _disable_continue_workflow_button(self, *args):
        self._continue_workflow_button.disabled = True

    def _call_update_selected_node(self, change):
        """Display information relevant to newly selected node.
        To be called automatically by widget."""

        # Newly selected node (workflow step)
        # (Only take last if several are selected)
        if change['new'] is None:
            metadata = {}
            node = None
        else:
            node_num = change['new'][-1]

            node = self.nodes[node_num]

            with self.output_area:
                print("Selected node {}".format(node_num))

            metadata = node.get_user_dict()

        # Save current displayed task
        self.displayed_task = node

        # Update metadata and readme
        self._update_metadata_html(metadata)
        self._update_readme_html(node)
        self._update_log(node)

        # Update notebook link
        self.update_launch_notebook_link()

    def _read_log(self, log_path):
        try:
            with open(log_path) as log_file:
                log_text = log_file.read()
        except IOError:
            log_text = 'Error opening {}'.format(log_path)

        self._log_html.value = log_text

    def _call_read_log(self, caller=None):
        log_path = self._log_path_input.value
        self._read_log(log_path)

    def _run_wrapper(self, *args):
        with self._widget_log:
            try:
                if not self.future.running():
                    self.future = self._thread_pool.submit(self.run_workflow)
                    print("Workflow submitted.")
                else:
                    print("Workflow already running.")
            except AttributeError as e:
                # Attribute error if self.future is None
                # which means workflow has not been submitted.
                self.future = self._thread_pool.submit(self.run_workflow)

    def run_workflow(self, *args):
        """Run workflow with selected WorkerPool."""

        # Disable start job button upon submission
        self._run_button.disabled = True

        try:
            pool = self._worker_pool_selector.value

            with self._widget_log:
                print("Attempting to start job.")
                if pool is None:
                    print("No WorkerPool selected.")
                else:
                    pool.fw_run(self.workflow)

        finally:
            # Enable start job button after workflow is finished.
            # (finally ensures reenabling on success or failure)
            self._run_button.disabled = False
