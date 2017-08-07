# Oliver Evans
# August 7, 2017

import ipywidgets as ipw
import bqplot as bq

class EditHTML(ipw.VBox):
    def __init__(self, value='', text_height=400):
        super().__init__()
        self.HTML = ipw.HTMLMath(value=value)
        self.Text = ipw.Textarea(value=value)
        self.ToggleButton = ipw.Button(description='Toggle')
        
        self.elements = [self.HTML, self.Text]
        self.descriptions = ['Edit', 'Render']
        ipw.jslink((self.HTML, 'value'), (self.Text, 'value'))
        
        # Set height and width of Textarea
        self.Text.layout.height = u'{}px'.format(text_height)
        self.Text.layout.width = u'95%'
        
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
        super(WorkflowWidget, self).__init__()
        
        # Define variables
        self.workflow = workflow
        self._fig_layout = ipw.Layout(width='400px', height='600px')
        self._xs = bq.LinearScale()
        self._ys = bq.LinearScale()
        self._scales = {'x': self._xs, 'y': self._ys}
        mgin = 10
        
        # Define elements
        self._metadata_template = """
        Node name: {name}
        <br>
        Last modified: {date}
        <br>
        Description: {word}
        """
        self._metadata_html = ipw.HTML()
        
        readme_html = EditHTML(r"""
            <h1>Radiative Transfer</h1>

            The Radiative Transfer Equation is given by

            <p>
            $$\nabla I \cdot \omega = -c\, I(x, \omega) + \int_\Omega \beta(|\omega-\omega'|)\, I(x, \omega')$$
            </p>

            It is useful for
            <ul>
            <li>
            Stellar astrophysics
            </li>
            <li>
            Kelp
            </li>
            <li>
            Nice conversations
            </li>
            </ul>

            And is explained well by the following diagram.
            <br />
            <br />
            <img width=300px src="http://soap.siteturbine.com/uploaded_files/www.oceanopticsbook.info/images/WebBook/0dd27b964e95146d0af2052b67c7b5df.png" />
        """)
        self._notebook_button = ipw.Button(
            description='Open Notebook',
            button_style='success'
        )
        self._log_path_input = ipw.Text(
            description='Log path',
            value='/etc/login.defs'
        )
        self._log_html = ipw.HTML()
        
        self._readme_area = ipw.VBox([
            readme_html
        ])
        self._info_area = ipw.VBox([
            self._notebook_button,
            self._metadata_html
        ])
        self._log_area = ipw.VBox([
            self._log_path_input,
            self._log_html
        ])
        
        self._graph_container = workflow.draw_dag(layout=self._fig_layout)
        self._graph_figure = self._graph_container.children[0]
        
        self._tab = ipw.Tab([
            self._readme_area,
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
        self._tab.set_title(0, 'Readme')
        self._tab.set_title(1, 'Info')
        self._tab.set_title(2, 'Log')
        self._tab.layout.height = self._fig_layout.height
        self._tab.layout.width = self._fig_layout.width
        
        #self._graph_figure.layout.border = '3px red solid'
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
        self.bqgraph.observe(self._call_update_metadata_html, names='selected')
        self._log_path_input.on_submit(self._call_read_log)
        
        # Run updates
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
        
       
    def _call_update_metadata_html(self, change):
        # Newly selected node (workflow step)
        # (Only take first if several are selected)
        
        if change['new'] is None:
            metadata = {}
            
        else:
            node_num = change['new'][0]

            node = self.workflow.dag.nodes()[node_num]

            with self.output_area:
                print("Selected node {}".format(node_num))

            metadata = node.get_user_dict()

        self._update_metadata_html(metadata)

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
