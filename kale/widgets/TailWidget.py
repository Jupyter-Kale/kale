# stdlib
import time
from concurrent.futures import ThreadPoolExecutor

# 3rd party
import ipywidgets as ipw

# local
from . import utils

class TailWidget(ipw.VBox):
    """Tail a file. (traitfully!)"""

    def __init__(self, path='', width=500, padding=10):
        super().__init__()

        self.path = ipw.Text(
            description='File path',
            value=path,
            disabled=False
        )
        self.dt = ipw.FloatText(
            description='dt',
            value=1,
            layout=ipw.Layout(
                width='150px'
            )
        )
        self.num_lines = ipw.IntText(
            description='# of lines',
            value=20,
            layout=ipw.Layout(
                width='150px'
            )
        )
        self.start_button = ipw.Button(
            description='Start',
            button_style='success'
        )

        self.text = ipw.HTML()

        body_width=width-2*padding -2
        # TODO - not used
        button_width=100

        inner_layout = ipw.Layout(
            width='{}px'.format(body_width)
        )

        self.controls = ipw.VBox([
            ipw.HBox([
                self.path,
                self.start_button,
            ], layout=inner_layout),
            ipw.HBox([
                self.dt,
                self.num_lines
            ], layout=inner_layout)
        ])


        self._text_container = ipw.Box(
            [self.text],
            layout=ipw.Layout(
                padding='{}px'.format(padding),
                border='1px lightgray solid',
                width='{}px'.format(body_width)
            )
        )

        self.layout=ipw.Layout(
            padding='{}px'.format(padding),
            border='1px lightgray solid',
            width='{}px'.format(width)

        )

        self.children = [
            ipw.HTML("<b>File Tailer</b>"),
            self.controls,
            utils.Space(padding),
            self._text_container
        ]

        self._button_texts = ["Start", "Stop"]
        self._button_styles = ["success", "danger"]
        # Start off with button saying "Start"
        self._button_state = 0
        self.keep_watching = False

        self.thread_pool = ThreadPoolExecutor()

        # Future returned by watching process
        self.future = None

        # Logic
        self.start_button.on_click(self.click_button)

    def tail(self):
        """Tail file once and print to text area."""
        try:
            with open(self.path.value) as fh:
                text = '<br>'.join(fh.readlines()[-self.num_lines.value:])
        except IOError:
            text = "Error opening '{}'".format(self.path.value)
            self._toggle_button()

        self.text.value = text

    def watch_file(self):
        """Periodically read file and print to text area."""
        while self.keep_watching:
            self.tail()
            time.sleep(self.dt.value)

    def _set_button_state(self, state):
        """Set button state."""
        self._button_state = state
        self.start_button.description = self._button_texts[state]
        self.start_button.button_style = self._button_styles[state]
        self.keep_watching = bool(state)

    def _toggle_button(self):
        """Flip button state."""
        self._set_button_state((self._button_state+1)%2)

    def click_button(self,*args):
        """Toggle button and watch file when button is clicked."""
        self._toggle_button()
        self.future = self.thread_pool.submit(self.watch_file)
