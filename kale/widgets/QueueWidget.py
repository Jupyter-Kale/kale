# stdlib
import time
import subprocess

# 3rd party
import ipywidgets as ipw
import IPython.core.display as disp
from concurrent.futures import ThreadPoolExecutor
import pandas as pd


class QueueWidget(ipw.VBox):
    "Widget to show SLURM queue, allowing search by user."

    def __init__(self, auth_widget, user='oevans'):
        "QueueWidget constructor."
        # Call VBox constuctor
        super().__init__()

        # Variables
        self._refresh_text = "Refresh"
        self._loading_text = "Loading..."
        self._current_queue = pd.DataFrame()
        self._poll_delay = 5

        # Authenticated NEWTAuthWidget
        self._auth_widget = auth_widget

        # Define GUI elements
        self._user_input = ipw.Text(description='User', value=user)
        self._output_area = ipw.Output()
        self._refresh_button = ipw.Button(description=self._refresh_text)
        self._watch_checkbox = ipw.Checkbox(description='Watch?', value=False)
        # Seems to crash kernel - disable for now.
        self._watch_checkbox.disabled = False

        # Elements which should be disabled when GUI is disabled
        self._ui_elements = [
            self._user_input,
            self._refresh_button,
            self._watch_checkbox
            ]
        self._ui_disabled_states = [el.disabled for el in self._ui_elements]

        # Also create a log viewer which is not attached to the widget
        self._log = ipw.Output()

        # Horizontal container for refresh button and checkbox
        self._bottom_container = ipw.HBox([
            self._refresh_button,
            self._watch_checkbox
            ])

        # Define layout
        self.children = [
            self._user_input,
            self._output_area,
            self._bottom_container
            ]

        # Create thread pool for concurrent polling
        self._thread_pool = ThreadPoolExecutor()

        # Connnect GUI to model
        self._user_input.on_submit(self._disable_and_refresh)
        self._refresh_button.on_click(self._disable_and_refresh)
        self._watch_checkbox.observe(self._react_to_watch_checkbox, names='value')

        # Search queue with default user
        self._refresh_queue()

    def _disable_gui(self):
        "Disable GUI elements while loading."

        for el_num, el in enumerate(self._ui_elements):
            # Save current disabled state to revert to
            # (don't want to re-enable a widget which
            # should be disabled)
            self._ui_disabled_states[el_num] = el.disabled
            el.disabled = True

        self._refresh_button.description = self._loading_text

    def _enable_gui(self):
        "Enable GUI elements after loading."

        for el_num, el in enumerate(self._ui_elements):
            el.disabled = self._ui_disabled_states[el_num]

        self._refresh_button.description = self._refresh_text

    def _disable_and_refresh(self, caller=None):
        "Refresh SLURM squeue and disable GUI. Called by text submit or button click."

        # Disable GUI while queue is loading
        self._disable_gui()

        self._refresh_queue()

        # Enable GUI after table is displayed
        self._enable_gui()

    def _refresh_queue(self):
        "Refresh SLURM squeue display."

        with self._log:
            print("Polling now..")

        # Retreive queue data
        # queue_df = QueueWidget.squeue(self._user_input.value)

        session = self._auth_widget._session

        query_filter = dict(
            user=self._user_input.value
            )

        return_fields = [
            'user',
            'status',
            # 'repo',
            # 'rank_bf',
            'qos',
            'name',
            'timeuse',
            # 'source',
            'hostname',
            'jobid',
            'queue',
            'submittime',
            # 'reason',
            'memory',
            'nodes',
            'timereq',
            'procs',
            # 'rank_p'
            ]

        queue_df = self.newt_queue(session, query_filter, return_fields)

        # If request failed, stop trying to display
        if queue_df is None:
            return

        # Determine whether new queue is different than previous
        # Comparing dataframes returns a boolean dataframe
        # .all().all() reduces boolean DataFrame along both dimensions
        # to a single boolean value.
        # Using not outside instead of != inside to allow for empty DF == empty DF
        try:
            new_data = not (queue_df == self._current_queue).all().all()

        # If dataframes have different column headers, they aren't comparable,
        # and a ValueError will be raised, in which case they aren't the same.
        except ValueError:
            new_data = True

        # Redraw table if new data is present
        if new_data:
            with self._log:
                print("New data!")
            self._output_area.clear_output()
            with self._output_area:
                disp.display(queue_df)
        else:
            with self._log:
                print("No new data.")

        self._current_queue = queue_df

    def _watch_queue(self):
        "Refresh queue every `self._poll_delay` seconds."

        # Log beginning
        with self._log:
            print("Beginning polling.\n")

        # Stop once checkbox is turned off
        while self._watch_checkbox.value:
            self._refresh_queue()
            time.sleep(self._poll_delay)

        # Log end of polling
        with self._log:
            print("Ending polling.\n")

    def _react_to_watch_checkbox(self, change):
        "Start polling queue when checkbox is enabled"

        if change['new']:
            self._thread_pool.submit(self._watch_queue)

    def newt_queue(self, session, query_filter, return_fields, machine='cori'):
        """
        Use NEWT to query specified fields from an authenticated session, filtered as desired.
        session - authenticated requests session
        query_filter - dict of search fields and values to filter results (e.g. username, date)
        return_fields - fields to return for each job
        machine - NERSC machine to query

        returns a pandas DataFrame
        """

        base_url = "https://newt.nersc.gov/newt/"
        url_template = base_url + "queue/{machine}?{query_filter}"
        query_filter = '&'.join(['{}={}'.format(*item) for item in query_filter.items() if item[1] != ''])

        full_url = url_template.format(
            machine=machine,
            query_filter=query_filter
            )

        request = session.get(full_url)

        try:
            jobs = request.json()

            if self._auth_widget.check_auth():
                return pd.DataFrame(
                    data=[[job[key] for key in return_fields] for job in jobs],
                    columns=return_fields
                    )
            else:
                # Display not authenticated warning

                html_output = """\
                <div class="alert alert-warning">
                Not authenticated.
                </div>
                """

                self._output_area.clear_output()

                with self._output_area:
                    disp.display(
                        disp.HTML(
                            html_output
                            )
                        )


        except ValueError:
            self._display_error(request)

    def _display_error(self, request):
        self._output_area.clear_output()
        with self._output_area:
            disp.display(disp.HTML(request.text))

    @staticmethod
    def squeue(user=''):
        "Query SLURM squeue for jobs by a particular user as pandas DataFrame."

        if len(user.split()) < 1:
            cmd_str = 'squeue'
        else:
            cmd_str = 'squeue -u {user}'.format(user=user)
        table_string, err = QueueWidget.shell(cmd_str)

        if err is not None:
            raise OSError(err)

        table_lists = [l.split() for l in table_string.split('\n')[:-1] if len(l.split()) == 8]
        table_header = table_lists[0]
        table_data = table_lists[1:]

        return pd.DataFrame(table_data, columns=table_header)

    @staticmethod
    def shell(cmd_str):
        "Run system command and return (sterr, stdout) as tuple of strings."

        cmd_list = cmd_str.split()
        proc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE)

        # Convert stdout and stderr from bytes to str
        stdout, stderr = (b.decode('utf-8') if type(b) is bytes else None for b in proc.communicate())

        return stdout, stderr
