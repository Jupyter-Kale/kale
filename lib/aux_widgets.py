# Oliver Evans
# August 7, 2017

# Auxillary widgets (NEWT, SLURM, etc.)

import io
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import json

import numpy as np
import ipyvolume.pylab as p3
import pandas as pd
import subprocess as sp
import IPython
import ipyvolume as ipv
import itertools as it
import ipywidgets as ipw
import IPython.core.display as disp
import requests
import paramiko
import traitlets
from weakref import WeakSet

## NEWT ##

class NEWTAuthWidget(ipw.VBox):
    def __init__(self):
        # Call VBox constructor
        super().__init__()
        
        # Variables
        title_html = """\
        <h3>NEWT Authenticator</h3>
        """
        
        # Define widgets
        self._title_widget = ipw.HTML(value=title_html)
        self._user_input = ipw.Text(description='Username')
        self._password_input = ipw.Password(description='Password')
        self._login_button = ipw.Button(description='Login')
        self._logout_button = ipw.Button(description='Logout', disabled=True)
        self._results_box = ipw.Output()
        
        # NEWT Session object
        self._session = requests.Session()
            
        # Define layout
        self.children = [
            self._title_widget,
            ipw.HBox([
                ipw.VBox([
                    self._user_input,
                    self._password_input,
                ]),
                ipw.VBox([
                    self._login_button,
                    self._logout_button
                ])
            ]),
            self._results_box
        ]
        
        # Logical group of login UI elements
        self._login_elements = [
            self._user_input,
            self._password_input,
            self._login_button
        ]
        
        # Connect UI elements to logic functions
        self._user_input.on_submit(self._login)
        self._password_input.on_submit(self._login)
        self._login_button.on_click(self._login)
        
        self._logout_button.on_click(self._logout)
                
        
    def _login(self, caller):
        "To be called by UI elements. Request authorization, return request object, and reset password field."
        
        # Get username and password
        username = self._user_input.value
        password = self._password_input.value
        
        # Reset password
        self._password_input.value = ''
        
        # Disable further login
        self._disable_login()
        
        # Submit authorization request
        request = self._authorize(self._session, username, password)
        
        # Verify auth request
        request_success = self._verify_auth_request(request)
        
        if request_success:
            # Enable logout
            self._enable_logout()
        else:
            # Reopen login form
            self._enable_login()
        
        # Update results box
        self._display_request(request, request_type='login')
    
    def _logout(self, caller):
        "Exit current NEWT auth session"
        
        # Submit logout request
        request = self._deauthorize(self._session)
        
        # Update results box
        self._display_request(request, request_type='logout')
        
        # Return GUI to login state
        self._disable_logout()
        self._enable_login()
         
    def _verify_auth_request(self, request):
        "Determine whether authentication request was successful"
        
        return request.json()['auth']
    
    def check_auth(self):
        "Determine whether session is authenticated."
        
        return self._verify_auth_request(
            self._session.get("https://newt.nersc.gov/newt/auth")
        )
        
    def _disable_login(self):
        "Disable login form"
        
        for el in self._login_elements:
            el.disabled = True
        
    def _enable_login(self):
        "Enable login form"
        
        for el in self._login_elements:
            el.disabled = False
        
    def _disable_logout(self):
        "Disable logout button"
        
        self._logout_button.disabled = True
        
    def _enable_logout(self):
        "Enable logout button"
        
        self._logout_button.disabled = False
        
    def _display_request(self, request, request_type):
        "Update results box to display result of requested login or logout."
        
        self._results_box.clear_output()
        
        with self._results_box:
            if request_type == 'login':
                self._render_login_status(request)
            elif request_type == 'logout':
                self._render_logout_status(request)
            else:
                raise ValueError("Unknown NEWT request type")
            self._render_auth_table(request)
    
    def _request_json_to_html(self, request):
        "Convert json (python dict) of request to a nice HTML format"
        
        df = pd.DataFrame(list(request.json().items()))
        return df.to_html(index=False, header=False)
    
    def _render_login_status(self, request):
        "Write success/failure message to the results_box."
        
        status = self._verify_auth_request(request)
        
        if status:
            result = "Success!"
            msg_type = "success"
        else:
            result = "Failure."
            # msg_type = "warning"
            msg_type = "danger"
            
        html_output = """\
        <div class="alert alert-{msg_type}">
        Login {result}
        </div>
        """.format(
            result=result,
            msg_type=msg_type
        )
        
        with self._results_box:
            disp.display(
                disp.HTML(
                    html_output
                )
            )
    def _render_logout_status(self, request):
        "Write success/failure message to the results_box."
        
        status = self._verify_auth_request(request)
        
        if not status:
            result = "Success!"
            msg_type = "info"
        else:
            result = "Failure."
            msg_type = "danger"
            
        html_output = """\
        <div class="alert alert-{msg_type}">
        Logout {result}
        </div>
        """.format(
            result=result,
            msg_type=msg_type
        )
        
        with self._results_box:
            disp.display(
                disp.HTML(
                    html_output
                )
            )
            
    def _render_auth_table(self, request):
        disp.display(disp.HTML(
                self._request_json_to_html(request)
            ))
        
    def _authorize(self, session, username, password):
        "Send username & password, and request authorization"
        
        return session.post("https://newt.nersc.gov/newt/auth", {"username": username, "password": password})
        
        
    def _deauthorize(self, session):
        "Send logout request"
        
        return session.get("https://newt.nersc.gov/newt/logout")

class SSHAuthWidget(ipw.VBox):
    "Connect to host via SSH (paramiko)"

    # Class attribute - dict of widgets with open connections
    # connect_string: widget
    open_connections = {}

    # Instances of this class
    # WeakSet so that this doesn't prevent garbage collection
    # https://stackoverflow.com/questions/12101958/keep-track-of-instances-in-python
    #instances = WeakSet()

    def __init__(self, host='', username=''):
        # Call VBox constructor
        super().__init__()
        
        # Variables
        title_html = """\
        <h3>SSH Authenticator</h3>
        """
        
        # Define widgets
        self._title_widget = ipw.HTML(value=title_html)
        self._host_input = ipw.Text(description='Host URL', value=host)
        self._user_input = ipw.Text(description='Username', value=username)
        self._password_input = ipw.Password(description='Password')
        self._login_button = ipw.Button(description='Login')
        self._logout_button = ipw.Button(description='Logout', disabled=True)
        self._results_box = ipw.Output()

        # user@host
        self.connect_string = ''
        
        # SSH Client object
        self.client = paramiko.SSHClient()
        
        # Start out unauthorized
        self.authorized = False

        # Load SSH known_hosts
        self.client.load_system_host_keys()
            
        # Define layout
        self.children = [
            self._title_widget,
            ipw.HBox([
                ipw.VBox([
                    self._host_input,
                    self._user_input,
                    self._password_input,
                ]),
                ipw.VBox([
                    self._login_button,
                    self._logout_button
                ])
            ]),
            self._results_box
        ]
        
        # Logical group of login UI elements
        self._login_elements = [
            self._user_input,
            self._password_input,
            self._login_button
        ]
        
        # Connect UI elements to logic functions
        self._user_input.on_submit(self._login)
        self._password_input.on_submit(self._login)
        self._login_button.on_click(self._login)
        self._logout_button.on_click(self._logout)
                
        
    def _login(self, caller):
        "To be called by UI elements. Request authorization, return request object, and reset password field."
        
        # Get username and password
        host = self._host_input.value
        username = self._user_input.value
        password = self._password_input.value
        
        # Reset password
        self._password_input.value = ''
        
        # Temporarily disable further login
        self._disable_login()
        
        # Verify auth request
        try:
            request_success = self._authorize(host, username, password)
        except Exception as e:
            self._enable_login()
            self._disable_logout()
            raise e
        
        if request_success:
            # Enable logout
            self._enable_logout()

        else:
            # Reopen login form
            self._enable_login()
        
        # Update results box
        #self._display_request(request, request_type='login')
    
    def _logout(self, caller):
        "Exit current NEWT auth session"
        
        # Submit logout request
        self._deauthorize()
        
        # Update results box
        #self._display_request(request, request_type='logout')
        
        # Return GUI to login state
        self._disable_logout()
        self._enable_login()

        self.connect_string = ''
         
    def check_auth(self):
        "Determine whether session is authenticated."

        return self.authorized
        
    def _disable_login(self):
        "Disable login form"
        
        for el in self._login_elements:
            el.disabled = True
        
    def _enable_login(self):
        "Enable login form"
        
        for el in self._login_elements:
            el.disabled = False
        
    def _disable_logout(self):
        "Disable logout button"
        
        self._logout_button.disabled = True
        
    def _enable_logout(self):
        "Enable logout button"
        
        self._logout_button.disabled = False
        
    def _display_request(self, request, request_type):
        "Update results box to display result of requested login or logout."
        
        self._results_box.clear_output()
        
        with self._results_box:
            if request_type == 'login':
                self._render_login_status(request)
            elif request_type == 'logout':
                self._render_logout_status(request)
            else:
                raise ValueError("Unknown NEWT request type")
            self._render_auth_table(request)
    
    def _request_json_to_html(self, request):
        "Convert json (python dict) of request to a nice HTML format"
        
        df = pd.DataFrame(list(request.json().items()))
        return df.to_html(index=False, header=False)
    
    def _render_login_status(self):
        "Write success/failure message to the results_box."
        
        status = self.authorized
        
        if status:
            result = "Success!"
            msg_type = "success"
        else:
            result = "Failure."
            # msg_type = "warning"
            msg_type = "danger"
            
        html_output = """\
        <div class="alert alert-{msg_type}">
        Login {result}
        </div>
        """.format(
            result=result,
            msg_type=msg_type
        )
        
        with self._results_box:
            disp.display(
                disp.HTML(
                    html_output
                )
            )
    def _render_logout_status(self):
        "Write success/failure message to the results_box."
        
        status = self.authorized
        
        if not status:
            result = "Success!"
            msg_type = "info"
        else:
            result = "Failure."
            msg_type = "danger"
            
        html_output = """\
        <div class="alert alert-{msg_type}">
        Logout {result}
        </div>
        """.format(
            result=result,
            msg_type=msg_type
        )
        
        with self._results_box:
            disp.display(
                disp.HTML(
                    html_output
                )
            )
            
    def _render_auth_table(self, request):
        disp.display(disp.HTML(
                self._request_json_to_html(request)
            ))
        
    def _authorize(self, host, username, password):
        "Send username & password, and request authorization"
        
        try:
            self.client.connect(
                hostname=host,
                username=username,
                password=password
            )
            self.connect_string = '{}@{}'.format(username, host)
            self._register()

            success = True
        except paramiko.AuthenticationException:
            success = False

        return success
        
    def _deauthorize(self):
        "Send logout request"
        self.client.close()
        self._unregister()
    
    def _register(self):
        "Add authenticated widget (self) to class list of open SSH connections."
        self.__class__.open_connections[self.connect_string] = self

    def _unregister(self):
        "Remove widget (self) from class list of open SSH connections."
        self.__class__.open_connections.pop(self.connect_string)



class SSHTerminal(ipw.VBox):
    "Simple terminal widget over SSH."
    def __init__(self, ssh_widget, width=500):
        super().__init__()
        
        self.ssh_widget = ssh_widget
        #self.shell_channel = ssh_widget.client.invoke_shell()
        
        self.stdin = None
        self.stdout = None
        self.stderr = None
        
        header_height = 30

        # Maximum number of bytes to read
        self.max_bytes = int(1e6)
        
        self._header = ipw.HTML("<b>SSH Terminal</b>")
        self.output = ipw.HTML(
        #self.output = ipw.Output(
            layout=ipw.Layout(
                height='{}px'.format(300-header_height-2)
            )
        )
        self._output_container = ipw.VBox(
            [self.output],
            layout=ipw.Layout(
                border='1px lightgray solid',
                width='{}px'.format(width),
                height='{}px'.format(300-header_height)
            )
        )
        self._prompt = ipw.HTML(
            value=self._terminal_font(
               self._terminal_font(self.ssh_widget.connect_string+'$') 
            )
        )
        self.input = ipw.Text()
        self._input_container = ipw.HBox([
            self._prompt,
            self.input
        ])
        
        self.children = [
            self._header,
            self._output_container,
            self._input_container
        ]
        
        self.input.on_submit(self._submit_input)

        #self.display_shell_output()

        # Create thread pool for concurrent polling
        #self._thread_pool = ThreadPoolExecutor()
        
    def run_exec(self, command):
        "Submit command via exec_command."
        self.stdin, self.stdout, self.stderr = self.ssh_widget.client.exec_command(command)

    def run_shell(self, command):
        "Submit command via invoke_shell."
        self.shell_channel.send(command+"\n")

    def display_shell_output(self):
        "Display output from invoke_shell."
        response = self.shell_channel.recv(self.max_bytes)

        with self.output:
            print(response.decode())
        
    def display_exec_output(self):
        "Display output from exec_command."
        stdout = '<br>'.join(self.stdout.readlines())
        stderr = '<br>'.join(self.stderr.readlines())
        
        inp = self._terminal_font(self.input.value, color='green')
        output = []
        
        if len(stdout) > 0:
            output.append(self._terminal_font(stdout))
            
        if len(stderr) > 0:
            output.append(self._terminal_font(stderr, 'red'))
        
        output.append('<br>')
            
        self.output.value += (
            inp + '<br>'.join(output)
        )
    
    def _terminal_font(self, text, color=None):
        style = "font-family: monospace; "
        
        if color is not None:
            style += "color: {}; ".format(color)
        
        html = "<p style='{style}'>{text}</p>".format(
            style=style,
            text=text
        )
        
        return html
        
    def _submit_input(self, *args, **kwargs):
        #self.run_shell(self.input.value)
        self.run_exec(self.input.value)
        #self._poll_response()
        self.display_exec_output()
        self.input.value = ''

    def _poll_response(self, timeout=1):
        """Continuously look for response from server until timeout has expired 
        with no new data."""

        keep_polling = True

        # Keep polling as long as new data is available every `timeout` seconds
        while self.shell_channel.recv_ready():
            self.display_shell_output()
            time.sleep(timeout)


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
            user = self._user_input.value
        )
        
        return_fields = [
            'user',
            'status',
            #'repo',
            #'rank_bf',
            'qos',
            'name',
            'timeuse',
            #'source',
            'hostname',
            'jobid',
            'queue',
            'submittime',
            #'reason',
            'memory',
            'nodes',
            'timereq',
            'procs',
            #'rank_p'
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
        proc = sp.Popen(cmd_list, stdout=sp.PIPE)
        
        # Convert stdout and stderr from bytes to str
        stdout, stderr = (b.decode('utf-8') if type(b) is bytes else None for b in proc.communicate())
        
        return stdout, stderr

## File Browser ##

class FileBrowserWidget(ipw.Select):
    def __init__(self):
        super().__init__()
        
        self.cwd = os.getcwd()
        self.options = self.ls()
        
        self.layout.height = u'300px'
        
        self.observe(self.perform_cd, names='index')
        
    
    def ls(self):
        full_ls = os.listdir(self.cwd)
        
        dirs = []
        files = []
        for file in full_ls:
            abspath = self.abspath(file)
            
            if os.path.isdir(abspath):
                dirs.append(file)
            else:
                files.append(file)

        # Format
        dirs = [self.format_dir(path) for path in self.sortfiles(dirs)]
        files = [self.format_file(path) for path in self.sortfiles(files)]
            
        return tuple([('.', '.'),('..', '..')] + dirs + files)
    
    def format_dir(self, path):
        return (self.dirify(path), self.abspath(path))
    
    def format_file(self, path):
        return (path, self.abspath(path))
    
    def sortfiles(self, filelist):
        "Sort files alphabetically, placing hidden files after others."
        
        hidden = []
        nonhidden = []
        
        for file in filelist:
            if file[0] == '.':
                hidden.append(file)
            else:
                nonhidden.append(file)
        
        return sorted(nonhidden) + sorted(hidden)
    
    def dirify(self, path):
        "How to represent directory names"
        return '** {} **'.format(path)
    
    def abspath(self, path):
        "Join path to cwd and return absolute path"
        return os.path.abspath(
            os.path.join(
                self.cwd,
                path
            )
        )
    
    def cd(self, path):
        self.cwd = self.abspath(path)
        self.options = self.ls()
    
    def perform_cd(self, change):
        newindex = change['new']
        newdir = self.options[newindex][1]
        if os.path.isdir(newdir):
            self.cd(newdir)

def Space(height=0, width=0):
    "Empty IPyWidget Box of specified size."
    return ipw.Box(
        layout=ipw.Layout(
            width=u'{}px'.format(width),
            height=u'{}px'.format(height)
        )
    )

## Table

class RowWidget(ipw.HBox):
    "Row associated with one table."
    def __init__(self, row, table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.children = self.parse_row(row)
        self.layout = table.row_layout
            
    def parse_row(self, row):
        "Create usable widgets from row contents"
        cell_boxes = []
        for i, cell in enumerate(row):
            if type(cell) is str:
                cell_widget = ipw.HTML(cell)
            else:
                cell_widget = cell
                
            cell_boxes.append(
                ipw.HBox(
                    [cell_widget],
                    layout=self.table.cell_layouts[i],
                )
            )
        
        return cell_boxes
    
    def get_index(self):
        "Get index of this row in the table"
        return self.table.children.index(self)

class TableWidget(ipw.VBox):
    def __init__(self, contents, align='left', width=600, col_widths=None):
        "Contents should be a rectangular list of lists or numpy array"
        super().__init__()
        
        if type(width) is int:
            self.width_u = u'{}px'.format(width)
            self.width_int = width
        else:
            try:
                if '%' in width:
                    raise NotImplementedError("Specify absolute width, not %.")
            except TypeError:
                pass
        
            self.width_u = u'{}'.format(width)
            self.width_int = int(width.strip('px'))
            
        self.num_cols = max([len(row) for row in contents])

        # By default, make columns equal in length
        if col_widths is None:
            try:
                self.col_width_int = self.width_int // self.num_cols
            except ZeroDivisionError:
                self.col_width_int = 0
            self.col_widths_int = [self.col_width_int] * self.num_cols
            self.col_widths_u = [u'{}px'.format(width_int) for width_int in self.col_widths_int]
        
        # If `col_widths` is specified, override `width`.
        else:
            try:
                if len(col_widths) == self.num_cols:
                    self.col_widths_int = col_widths
                    self.col_widths_u = [u'{}px'.format(width_int) for width_int in col_widths]
                    self.width_int = sum(col_widths)
                    self.width_u = u'{}px'.format(self.width_int)
                else:
                    raise ValueError("Incorrect number of column widths provided.")
            # If len(col_widths) doesn't make sense
            except TypeError:
                raise ValueError("col_widths should be an iterable.")

        # Row alignment
        if align == 'left':
            flex_align = 'flex-start'
        elif align == 'right':
            flex_align = 'flex-end'
        else:
            flex_align = align
        
        self.layout = ipw.Layout(
            width=self.width_u
        )
        
        self.row_layout = ipw.Layout(
            width=self.width_u,
            justify_content='space-between'
        )
        self.cell_layouts = [
            ipw.Layout(
                width=col_width,
                justify_content=flex_align
            )
            for col_width in self.col_widths_u
        ]
        
        self.from_lists(contents)
        
    def from_lists(self, contents):
        "Create table from lists"
        for row in contents:
            self.add_row(row)
                
    def add_row(self, row):
        "Append row to end of table"
        row_box = RowWidget(row, table=self, layout=self.row_layout)
        self.children += (row_box,)
    
    def insert_row(self, index, row):
        "Insert `row` at before row `index`"
        row_box = RowWidget(row, table=self, layout=self.row_layout)
        new_children = list(self.children)
        new_children.insert(index, row_box)
        self.children = new_children
        
    def pop_row(self, index):
        "Pop row from table, as in `list.pop`."
        row_list = list(self.children)
        old = row_list.pop(index)
        self.children = row_list
        return old
