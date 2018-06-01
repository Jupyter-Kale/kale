
import traitlets
import ipywidgets as ipw

from .ssh import SSHAuthWidget
from .TableWidget import TableWidget

class WorkerPoolWidget(ipw.VBox):
    """GUI widget for managing WorkerPools."""

    _pool_list = traitlets.List()
    _pool_dict = traitlets.Dict()
    _workflow_widgets = traitlets.List()

    def __init__(self, fwconfig=None):

        # Keep track of available hosts
        self.ssh_hosts = SSHAuthWidget.open_connections

        # UI
        self.out_area = ipw.Output()
        self._name_text = ipw.Text()
        self._location_text = ipw.Dropdown(
            options=self.get_locations()
        )
        self._num_workers_text = ipw.IntText(value=1)
        self._new_button = ipw.Button(
            icon="plus",
            button_style="success",
        )

        self._header = ipw.HTML("<h3>Worker Pools</h3>")
        self.table = TableWidget(
            [["<b>Name</b>", "<b>Location</b>",
            "<b>Workers</b>", "<b>Action</b>"],
            [self._name_text, self._location_text,
            self._num_workers_text, self._new_button]],

            col_widths=[150, 200, 60, 100]
        )
        self._status_bar = ipw.HTML()

        # Layout
        # IntText needs to be 2 pixels smaller than its container
        name_text_width = self.table.col_widths_int[0]-2
        location_text_width = self.table.col_widths_int[1]-2
        int_text_width = self.table.col_widths_int[2]-2
        self._name_text.layout=ipw.Layout(
            width=u'{}px'.format(name_text_width)
        )
        self._location_text.layout=ipw.Layout(
            width=u'{}px'.format(location_text_width)
        )
        self._num_workers_text.layout=ipw.Layout(
            width=u'{}px'.format(int_text_width)
        )

        # Traits
        self._pool_dict = {}
        self._pool_list = []
        self._workflow_widgets = []

        # Logic
        self._name_text.on_submit(self._watch_add_pool)
        self._new_button.on_click(self._watch_add_pool)

        super().__init__(
            children=[self._header, self.table, self._status_bar]
        )

        self._fwconfig = fwconfig

        # Add default pool
        self.add_pool('default', multiprocessing.cpu_count())

    def get_locations(self):
        """Get locations where workers can be created."""
        return ['localhost'] + list(self.ssh_hosts.keys())

    def add_pool(self, name, num_workers, location='localhost'):
        """Add WorkerPool with name `name` and `num_workers` workers to widget."""
        # Check for name conflicts
        if name in self._pool_dict.keys():
            self.set_status(
                text="Pool with name '{}' already defined in this widget.".format(name),
                alert_style="danger"
            )
        else:

            #with self.out_area:
            pool = kale.workflow_objects.WorkerPool(name, num_workers, self._fwconfig, location)

            remove_button = ipw.Button(
                description="Remove",
                button_style="danger"
            )

            self._pool_dict[name] = pool
            self._pool_list.append(
                (pool.name, pool)
            )
            self.table.insert_row(
                -1,
                [name, location, str(num_workers), remove_button]
            )

            # Store row information in remove_button so that the
            # button can query the present row index when clicked
            # in order to remove the correct pool.

            # Newly created row is second to last (creation form is last)
            remove_button.row = self.table.children[-2]

            remove_button.on_click(self._remove_button)

            # Manually update widgets.
            self._update_worker_pool_list()

            self.set_status("WorkerPool '{}' created.".format(name), alert_style='success')

    def _watch_add_pool(self, caller):
        """Add worker pool to widget. To be called by button."""
        num_workers = self._num_workers_text.value
        location = self._location_text.value
        name = self._name_text.value
        self.add_pool(name, location=location, num_workers=num_workers)

        # Reset values
        self._num_workers_text.value = 1
        self._name_text.value = ''

    def get_pool(self, name):
        """Get worker pool by name"""
        return self._pool_dict[name]

    def pop_pool(self, index):
        """Remove worker pool in row `index` from widget, and return the pool, as in `list.pop`."""
        row = self.table.pop_row(index)
        name = row.children[0].children[0].value
        pool = self._pool_dict.pop(name)
        self._pool_list.pop(self._pool_list.index((pool.name, pool)))
        # Have to use '=' for .observe to be called
        self._pool_list = self._pool_list

        # Manually update widgets.
        self._update_worker_pool_list()

        self.set_status("WorkerPool '{}' removed.".format(name), alert_style='warning')

    def _update_worker_pool_list(self):
        """
        Due to a traitlets bug, we have to manually update
        the pool list in the WorkflowWidget.
        """
        for ww in self._workflow_widgets:
            ww._worker_pool_selector.options = self._pool_list

    def _remove_button(self, button):
        """To be called by remove button."""
        index = button.row.get_index()
        self.pop_pool(index)

    def set_status(self, text, alert_style='info'):
        if alert_style is None:
            css_class = ''
        elif alert_style in ['danger','warning','info','success']:
            css_class = 'alert alert-{}'.format(alert_style)
        else:
            # TODO - missing condition, need to determine what to do here
            raise Exception("Unrecognized alert_style {}".format(alert_style))

        self._status_bar.value="""
        <div class="{css_class}" style="width: {width}">
        {text}
        </div>
        """.format(
            css_class=css_class,
            width=self.table.layout.width,
            text=text
        )
