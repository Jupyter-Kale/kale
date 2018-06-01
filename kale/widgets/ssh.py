# stdlib
import time

# 3rd party
import ipywidgets as ipw
import IPython.core.display as disp
import paramiko
import pandas as pd

class SSHAuthWidget(ipw.VBox):
    "Connect to host via SSH (paramiko)"

    # Class attribute - dict of widgets with open connections
    # connect_string: widget
    open_connections = {}

    # Instances of this class
    # WeakSet so that this doesn't prevent garbage collection
    # https://stackoverflow.com/questions/12101958/keep-track-of-instances-in-python
    # instances = WeakSet()

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
        # self._display_request(request, request_type='login')

    def _logout(self, caller):
        "Exit current NEWT auth session"

        # Submit logout request
        self._deauthorize()

        # Update results box
        # self._display_request(request, request_type='logout')

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

        # TODO - _render_login_status() does not take arguments, but request is being passed?
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
        # self.shell_channel = ssh_widget.client.invoke_shell()

        self.stdin = None
        self.stdout = None
        self.stderr = None

        header_height = 30

        # Maximum number of bytes to read
        self.max_bytes = int(1e6)

        self._header = ipw.HTML("<b>SSH Terminal</b>")
        self.output = ipw.HTML(
            # self.output = ipw.Output(
            layout=ipw.Layout(
                height='{}px'.format(300 - header_height - 2)
                )
            )
        self._output_container = ipw.VBox(
            [self.output],
            layout=ipw.Layout(
                border='1px lightgray solid',
                width='{}px'.format(width),
                height='{}px'.format(300 - header_height)
                )
            )
        self._prompt = ipw.HTML(
            value=self._terminal_font(
                self._terminal_font(self.ssh_widget.connect_string + '$')
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

        # self.display_shell_output()

        # Create thread pool for concurrent polling
        # self._thread_pool = ThreadPoolExecutor()

    def run_exec(self, command):
        "Submit command via exec_command."
        self.stdin, self.stdout, self.stderr = self.ssh_widget.client.exec_command(command)

    def run_shell(self, command):
        "Submit command via invoke_shell."
        self.shell_channel.send(command + "\n")

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
        # self.run_shell(self.input.value)
        self.run_exec(self.input.value)
        # self._poll_response()
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
