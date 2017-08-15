# Oliver Evans
# August 7, 2017

import ipywidgets as ipw
import aux_widgets as aux
import workflow_objects as kale
import traitlets
import bqplot as bq

class EditHTML(ipw.VBox):
    def __init__(self, value='', text_height=400):
        super().__init__()
        self.HTML = ipw.HTMLMath(value=value)
        self.Text = ipw.Textarea(value=value)
        self.ToggleButton = ipw.Button(description='Toggle')
        
        self.elements = [self.HTML, self.Text]
        self.descriptions = ['Edit Description', 'Render Description']
        ipw.jslink((self.HTML, 'value'), (self.Text, 'value'))
        
        # Set height and width of Textarea
        self.Text.layout.height = u'{}px'.format(text_height)
        self.Text.layout.width = u'95%'
        self.HTML.layout.border=u'1px lightgray solid'
        self.HTML.layout.padding=u'10px'
        
        # Set HTML view by default
        self.set_view(0)
        
        self.ToggleButton.on_click(self.toggle)
    
    def set_view(self, state):
        self.state = state
        self.children = [self.elements[state], self.ToggleButton]
        self.ToggleButton.description = self.descriptions[state]
        
    def toggle(self, caller):
        self.set_view((self.state+1)%2)

class WorkflowWidget(ipw.HBox):
    "Widget to draw DAG via bqplot and provide node-level info/interaction."
    
    @property
    def bqgraph(self):
        return self.workflow.get_bqgraph()
    
    def __init__(self, workflow):
        super().__init__()
        
        # Define variables
        self.workflow = workflow
        self._fig_layout = ipw.Layout(width='400px', height='600px')
        self._xs = bq.LinearScale()
        self._ys = bq.LinearScale()
        self._scales = {'x': self._xs, 'y': self._ys}
        mgin = 10

        # Link which binds readme text of selected task to displayed value
        self._readme_link = None
        
        # Define elements
        self._metadata_template = """
        Node name: {name}
        <br>
        Last modified: {date}
        <br>
        Description: {word}
        """
        self._metadata_html = ipw.HTML()
        
        self._readme_html = EditHTML()
        self._notebook_button = ipw.Button(
            description='Open Notebook',
            button_style='success'
        )
        self._log_path_input = ipw.Text(
            description='Log path',
            value=''
        )
        self._log_html = ipw.HTML()
        
        self._readme_area = ipw.VBox([
            self._readme_html
        ])
        self._info_area = ipw.VBox([
            self._readme_html,
            aux.Space(height=20),
            self._metadata_html,
            self._notebook_button
        ])
        self._log_area = ipw.VBox([
            self._log_path_input,
            self._log_html
        ])
        
        self._graph_container = workflow.draw_dag(layout=self._fig_layout)
        self._graph_figure = self._graph_container.children[0]
        
        self._tab = ipw.Tab([
            #self._readme_area,
            self._info_area,
            self._log_area
        ])
        
        self.output_area = ipw.Output()
        
        # Define layout
        self.children = [
            self._graph_container,
            self._tab,
        ]
        
        # Set attributes
        #self._tab.set_title(0, 'Readme')
        self._tab.set_title(0, 'Info')
        self._tab.set_title(1, 'Log')
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
        
        # Logic
        self.bqgraph.observe(self._call_update_selected_node, names='selected')
        self._log_path_input.on_submit(self._call_read_log)
        
        # Run updates
        self._update_readme_html()
        self._update_log()
    
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
        "Link README HTML to task."

        # Break previous link if it exists
        try:
            self._readme_link.unlink()
            self._readme_link = None
        except AttributeError:
            pass

        # Only create new link if a task is selected
        if task is None:
            #self._readme_html.HTML.value = "None selected."
            self._update_readme_html(self.workflow)
        else:
            # Update displayed HTML before linking
            self._readme_html.HTML.value = task.readme

            # Link values
            self._readme_link = traitlets.link(
                (self._readme_html.HTML, 'value'),
                (task, 'readme')
            )
       
    def _call_update_selected_node(self, change):
        """Display information relevant to newly selected node.
        To be called automatically by widget."""

        # Newly selected node (workflow step)
        # (Only take first if several are selected)
        if change['new'] is None:
            metadata = {}
            node = None
            
        else:
            node_num = change['new'][0]

            node = self.workflow.dag.nodes()[node_num]

            with self.output_area:
                print("Selected node {}".format(node_num))

            metadata = node.get_user_dict()

        # Update metadata and readme
        self._update_metadata_html(metadata)
        self._update_readme_html(node)
        self._update_log(node)

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


class WorkerPoolWidget(ipw.VBox):
    "GUI widget for managing WorkerPools."
    def __init__(self):
        
        # UI
        self.out_area = ipw.Output()
        self._name_text = ipw.Text()
        self._num_workers_text = ipw.IntText(value=1)
        self._new_button = ipw.Button(
            icon="plus",
            button_style="success",
        )

        self._header = ipw.HTML("<h3>Worker Pools</h3>")
        self.table = aux.TableWidget(
            [["<b>Name</b>", "<b>Workers</b>", "<b>Action</b>"],
            [self._name_text, self._num_workers_text, self._new_button]],
            col_widths=[150,60,100]
        )
        self._status_bar = ipw.HTML()

        # Layout
        # IntText needs to be 2 pixels smaller than its container
        name_text_width = self.table.col_widths_int[0]-2
        int_text_width = self.table.col_widths_int[1]-2
        self._name_text.layout=ipw.Layout(
            width=u'{}px'.format(name_text_width)
        )
        self._num_workers_text.layout=ipw.Layout(
            width=u'{}px'.format(int_text_width)
        )
        
        # Logic
        self._pool_dict = {}
        self._name_text.on_submit(self._watch_add_pool)
        self._new_button.on_click(self._watch_add_pool)
        
        super().__init__(
            children=[self._header, self.table, self._status_bar]
        )

    def add_pool(self, name, num_workers):
        "Add WorkerPool with name `name` and `num_workers` workers to widget."
        # Check for name conflicts
        if name in self._pool_dict.keys():
            self.set_status(
                text="Pool with name '{}' already defined in this widget.".format(name),
                alert_style="danger"
            )
        else:

            with self.out_area:
                pool = kale.WorkerPool(name, num_workers)

            remove_button = ipw.Button(
                description="Remove",
                button_style="danger"
            )

            self._pool_dict[name] = pool
            self.table.insert_row(
                -1,
                [name, str(num_workers), remove_button]
            )

            # Store row information in remove_button so that the 
            # button can query the present row index when clicked
            # in order to remove the correct pool.

            # Newly created row is second to last (creation form is last)
            remove_button.row = self.table.children[-2]

            remove_button.on_click(self._remove_button)

            self.set_status("WorkerPool '{}' created.".format(name), alert_style='success')
    
    def _watch_add_pool(self, caller):
        "Add worker pool to widget. To be called by button."
        num_workers = self._num_workers_text.value 
        name = self._name_text.value
        self.add_pool(name, num_workers)

        # Reset values
        self._num_workers_text.value = 1
        self._name_text.value = ''
        
    def get_pool(self, name):
        "Get worker pool by name"
        return self._pool_dict[name]
            
    def pop_pool(self, index):
        "Remove worker pool in row `index` from widget, and return the pool, as in `list.pop`."
        row = self.table.pop_row(index)
        name = row.children[0].children[0].value
        pool = self._pool_dict.pop(name)
        self.set_status("WorkerPool '{}' removed.".format(name), alert_style='warning')

    def _remove_button(self, button):
        "To be called by remove button."
        index = button.row.get_index()
        self.pop_pool(index)
    
    def set_status(self, text, alert_style='info'):
        if alert_style is None:
            css_class = ''
        elif alert_style in ['danger','warning','info','success']:
            css_class = 'alert alert-{}'.format(alert_style)
            
        self._status_bar.value="""
        <div class="{css_class}" style="width: {width}">
        {text}
        </div>
        """.format(
            css_class=css_class,
            width=self.table.layout.width,
            text=text
        )
   

