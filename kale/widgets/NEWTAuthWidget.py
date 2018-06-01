import ipywidgets as ipw
import IPython.core.display as disp
import pandas as pd
import requests


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

        # TODO - onsubmit is deprecated
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
