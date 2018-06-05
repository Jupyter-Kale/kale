# Oliver Evans
# August 7, 2017

# Widget to generate graphene sheets

import numpy as np
import ipyvolume.pylab as p3
import pandas as pd
import io
import os
import ipyvolume as ipv
import itertools as it
import ipywidgets as ipw
import time
from datetime import datetime

from kale.aux_widgets import Space
from .graphene_functions import *

class GrapheneSheetWidget(ipw.VBox):
    "Widget to specify parameters for an individual graphene sheet."
    
    def __init__(self, config=None):
        super(GrapheneSheetWidget, self).__init__()
        
        # Variables
        self.loc_x_range = (-10, 10)
        self.loc_y_range = (-10, 10)
        self.loc_z_range = (-10, 10)
        
        maxhex = 100
        
        self.bond_length_range = (1, 10)
        self.pair_length_range = (1, 10)
        
        self.bond_strength_expnt_range = (-5, 5)
        self.angle_strength_expnt_range = (-5, 5)
        self.pair_strength_expnt_range = (-5, 5)
        
        slider_style = dict(
            description_width = 'initial'
        )
        slider_layout = dict(
            max_width = '250px'
        )
        
        # Define UI elements
        loc_header = ipw.HBox(
            children=[ipw.HTML(
                "<strong>Location</strong>"
            )],
            layout=ipw.Layout(
                justify_content='flex-start'
            )
        )
        self._loc_x_slider = ipw.FloatSlider(
            min=self.loc_x_range[0], 
            max=self.loc_x_range[1], 
            description='x',
            style=slider_style,
            layout=slider_layout
        )
        self._loc_y_slider = ipw.FloatSlider(
            min=self.loc_y_range[0], 
            max=self.loc_y_range[1], 
            description='y',
            style=slider_style,
            layout=slider_layout
        )
        self._loc_z_slider = ipw.FloatSlider(
            min=self.loc_z_range[0], 
            max=self.loc_z_range[1], 
            description='z',
            style=slider_style,
            layout=slider_layout
        )
        
        num_header = ipw.HBox(
            children=[ipw.HTML(
                "<strong>Number of Hexagons</strong>"
            )],
            layout=ipw.Layout(
                justify_content='flex-start'
            )
        )
        # These must be odd numbers, hence step=2
        self._hex_x_slider = ipw.IntSlider(
            min=1,
            max=maxhex, 
            step=2,
            description='x',
            style=slider_style,
            layout=slider_layout
        )
        self._hex_y_slider = ipw.IntSlider(
            min=1, 
            max=maxhex,
            step=2,
            description='y',
            style=slider_style,
            layout=slider_layout
        )
        
        length_header = ipw.HBox(
            children=[ipw.HTML(
                "<strong>Potential Lengths</strong>"
            )],
            layout=ipw.Layout(
                justify_content='flex-start'
            )
        )
        self._bond_length_slider = ipw.FloatSlider(
            min=self.bond_length_range[0],
            max=self.bond_length_range[1],
            description='Bond',
            style=slider_style,
            layout=slider_layout
        )
        self._pair_length_slider = ipw.FloatSlider(
            min=self.pair_length_range[0],
            max=self.pair_length_range[1],
            description='Pair',
            style=slider_style,
            layout=slider_layout
        )
        
        strength_header = ipw.HBox(
            children=[ipw.HTML(
                "<strong>Potential Strengths Expnts.</strong>"
            )],
            layout=ipw.Layout(
                justify_content='flex-start'
            )
        )
        self._bond_strength_expnt_slider = ipw.FloatSlider(
            min=self.bond_strength_expnt_range[0],
            max=self.bond_strength_expnt_range[1],
            description='Bond',
            style=slider_style,
            layout=slider_layout
        )
        self._pair_strength_expnt_slider = ipw.FloatSlider(
            min=self.pair_strength_expnt_range[0],
            max=self.pair_strength_expnt_range[1],
            description='Pair',
            style=slider_style,
            layout=slider_layout
        )
        self._angle_strength_expnt_slider = ipw.FloatSlider(
            min=self.angle_strength_expnt_range[0],
            max=self.angle_strength_expnt_range[1],
            description='Angle',
            style=slider_style,
            layout=slider_layout
        )
        
        self._reset_button = ipw.Button(
            description="Reset",
            button_style="warning"
        )
        
        # Remove from controller.
        # Connected by controller, not by this widget
        self._remove_button = ipw.Button(
            description="Remove",
            button_style="danger"
        )
        
        reset_button_container = ipw.HBox(
            children=[self._reset_button],
            layout=ipw.Layout(
                justify_content='flex-start'
            )
        )
        
        self.children = [
            loc_header,
            self._loc_x_slider,
            self._loc_y_slider,
            self._loc_z_slider,
            
            num_header,
            self._hex_x_slider,
            self._hex_y_slider,

            length_header,
            self._bond_length_slider,
            self._pair_length_slider,

            strength_header,
            self._bond_strength_expnt_slider,
            self._pair_strength_expnt_slider,
            self._angle_strength_expnt_slider,

            self._reset_button,
            self._remove_button
        ]

        self.layout = ipw.Layout(
            display='flex',
            justify_content='space-between',
            width=u"302px"
        )
        
        # Logic
        self._reset_button.on_click(self._reset_sliders)

        # Set values on creation if config dict is provided
        if config is not None:
            self.set_values(config)
    
    def set_values(self, config):
        self._loc_x_slider.value = config['loc'][0]
        self._loc_y_slider.value = config['loc'][1]
        self._loc_z_slider.value = config['loc'][2]
        loc=[
            self._loc_x_slider.value,
            self._loc_y_slider.value,
            self._loc_z_slider.value,
        ],
        self._hex_x_slider.value = config['nx']
        self._hex_y_slider.value = config['ny']
        self._bond_length_slider.value = config['bond_length']
        self._pair_length_slider.value = config['pair_length']
        self._bond_strength_expnt_slider.value = np.log10(config['bond_strength'])
        self._pair_strength_expnt_slider.value = np.log10(config['pair_strength'])
        self._angle_strength_expnt_slider.value = np.log10(config['angle_strength'])
        
    def _reset_sliders(self, caller=None):
        self._loc_x_slider.value = 0
        self._loc_y_slider.value = 0
        self._loc_z_slider.value = 0
        self._bond_length_slider.value = 1
        self._pair_length_slider.value = 1
        self._bond_strength_expnt_slider.value = 0
        self._pair_strength_expnt_slider.value = 0
        self._angle_strength_expnt_slider.value = 0
    
    def generate_sheet_dict(self):
        return dict(
            loc=[
                self._loc_x_slider.value,
                self._loc_y_slider.value,
                self._loc_z_slider.value,
            ],
            nx = self._hex_x_slider.value,
            ny = self._hex_y_slider.value,
            bond_length = self._bond_length_slider.value,
            pair_length = self._pair_length_slider.value,
            bond_strength = 10 ** self._bond_strength_expnt_slider.value,
            pair_strength = 10 ** self._pair_strength_expnt_slider.value,
            angle_strength = 10 ** self._angle_strength_expnt_slider.value,
        )
        
class GrapheneControllerWidget(ipw.VBox):
    
    def __init__(self, auth_widget):
        super(GrapheneControllerWidget, self).__init__()
        
        # Variables
        maxbounds = [[-100, 100],
                     [-100, 100],
                     [-100, 100]]
        
        maxprocs = 32
        maxnodes = 32
        maxsteps = 1e20
        
        slider_style = dict(
            description_width = 'initial'
        )
        slider_layout = dict(
            max_width = '250px'
        )
        
        # UI Elements
        self._title = ipw.HTML("<h1>Graphene Simulator</strong>")
        self._accordion = ipw.Accordion()
        
        self._name_label = ipw.Label("Simulation Name")
        self._name = ipw.Text()
        
        self._base_dir_label = ipw.Label("Base Directory")
        self._base_dir = ipw.Text(value=os.getcwd())
        
        self._queue_label = ipw.Label("Queue")
        self._queue = ipw.Text('cori')
        
        self._partition_label = ipw.Label("Partition")
        self._partition = ipw.Text('debug')
        
        self._job_time_label = ipw.Label("Time Allocation")
        self._job_time = ipw.Text('10:00')
        
        self._nodes_label = ipw.Label("Number of Nodes")
        self._nodes = ipw.IntSlider(
            min=1, 
            max=maxnodes,
            layout=slider_layout,
            style=slider_style
        )
        
        self._bounds_label = ipw.Label("Simulation Bounds")
        self._x_bounds_slider = ipw.IntRangeSlider(
            min=maxbounds[0][0], 
            max=maxbounds[0][1], 
            description='x',
            layout=slider_layout,
            style=slider_style
        )
        self._y_bounds_slider = ipw.IntRangeSlider(
            min=maxbounds[1][0], 
            max=maxbounds[1][1], 
            description='y',
            layout=slider_layout,
            style=slider_style
        )
        self._z_bounds_slider = ipw.IntRangeSlider(
            min=maxbounds[2][0], 
            max=maxbounds[2][1], 
            description='z',
            layout=slider_layout,
            style=slider_style
        )
        
        self._procs_label = ipw.Label("Processor Grid")
        self._x_procs_slider = ipw.IntSlider(
            min=1, 
            max=maxprocs, 
            description='x',
            layout=slider_layout,
            style=slider_style
        )
        self._y_procs_slider = ipw.IntSlider(
            min=1, 
            max=maxprocs, 
            description='y',
            layout=slider_layout,
            style=slider_style
        )
        self._z_procs_slider = ipw.IntSlider(
            min=1, 
            max=maxprocs, 
            description='z',
            layout=slider_layout,
            style=slider_style
        )
        
        self._nsteps_label = ipw.Label("Number of Timesteps")
        self._nsteps = ipw.BoundedIntText(min=1, max=maxsteps, value=10000)
        
        self._dump_freq_label = ipw.Label("Dump Frequency")
        self._dump_freq = ipw.BoundedIntText(min=1, max=maxsteps, value=1000)
        
        
        self._sheet_widget_area = ipw.HBox()
        
        self._add_button = ipw.Button(
            description='Add Sheet',
            icon='plus', 
            button_style='success'
        )
        self._simulate_button = ipw.Button(
            description="Run simulation",
            button_style='success'
        )
        
        self._status_label = ipw.Label()

        self._log_area = ipw.Output()
        
        self._save_load_header = ipw.HTML('<strong>Save/Load Widget State</strong>')
        self._config_path_label = ipw.Label('Config file path')
        self._config_input = ipw.Text()
        self._load_config_button = ipw.Button(description='Load config')
        self._save_config_button = ipw.Button(description='Save config')
        
        # Define layout
        
        self._config_box = ipw.HBox([
            ipw.VBox([
                self._name_label,
                self._name,

                Space(10),
                self._base_dir_label,
                self._base_dir,

                Space(10),
                self._queue_label,
                self._queue,

                Space(10),
                self._partition_label,
                self._partition,

                Space(10),
                self._job_time_label,
                self._job_time,

                Space(10),
                self._nsteps_label,
                self._nsteps,

                Space(10),
                self._dump_freq_label,
                self._dump_freq,

                Space(20),
                ipw.HBox([
                    self._simulate_button,
                    Space(width=10),
                    self._status_label
                ])
            ]),
            
            Space(width=50),
            
            ipw.VBox([
                self._nodes_label,
                self._nodes,

                Space(10),
                self._procs_label,
                self._x_procs_slider,
                self._y_procs_slider,
                self._z_procs_slider,

                Space(10),
                self._bounds_label,
                self._x_bounds_slider,
                self._y_bounds_slider,
                self._z_bounds_slider,
                
                Space(20),
                self._save_load_header,
                self._config_path_label,
                self._config_input,
                ipw.HBox([
                    self._load_config_button,
                    self._save_config_button
                ])
            ])
        ])
        
        self._generator_area = ipw.VBox([
            self._add_button,
            self._sheet_widget_area
        ])
        
        self._accordion.children = [
            self._config_box,
            self._generator_area
        ]
        
        self.children = [
            self._title,
            self._accordion
        ]
        
        # Accordion titles
        self._accordion.set_title(0, 'Simulation Configuration')
        self._accordion.set_title(1, 'Graphene Generator')
        
        # Additional style
        #self._add_button.layout.width = u'36px'
        
        # Connect UI to logic
        self._add_button.on_click(self._add_sheet_widget)
        self._simulate_button.on_click(self.submit_simulation)
        self._name.observe(self._update_config_path)
        self._base_dir.observe(self._update_config_path)
        
        self._load_config_button.on_click(self._load_config_wrapper)
        self._save_config_button.on_click(self._save_config_wrapper)
        
        self._auth_widget = auth_widget
        self._update_config_path()
    
    def _add_sheet_widget(self, caller=None, config=None):
        print("Creating new widget.")

        new_widget = GrapheneSheetWidget(config)
        print("Created.")
        new_widget._remove_button.on_click(self._remove_sheet_widget)
        # Associate button with sheet widget to allow for removal by button click.
        new_widget._remove_button.parent_widget = new_widget
        print("Linked.")
        
        # Add to widget area
        self._sheet_widget_area.children = self._sheet_widget_area.children + (new_widget,)
        
    def _update_config_path(self, change=None):
        name = self._name.value
        self._config_input.value = os.path.join(
            self._base_dir.value,
            os.path.join(name, name+'.config')
        )
    
    def _remove_sheet_widget(self, caller):
        with self._log_area:
            print("Removing widget:")
            print(caller)
        
        # Function to exclude parent_widget of caller (remove button)
        # from children of sheet_widget_area
        
        exclude_widget = lambda widget: id(widget) != id(caller.parent_widget)
        self._sheet_widget_area.children = tuple(filter(exclude_widget, self._sheet_widget_area.children))
        
    def submit_simulation(self, caller):
        "Submit simulation given the simulation parameters in this widget."

        with self._log_area:
            simulate_graphene(
                job_name = self._name.value,
                auth_widget = self._auth_widget,
                sheet_dict_list = [
                    sheet.generate_sheet_dict() 
                    for sheet in self._sheet_widget_area.children
                ],
                nsteps = self._nsteps.value,
                dump_freq = self._dump_freq.value,
                base_dir = self._base_dir.value,
                bounds = np.array([
                    self._x_bounds_slider.value,
                    self._y_bounds_slider.value,
                    self._z_bounds_slider.value,
                ]),
                px = self._x_procs_slider.value,
                py = self._y_procs_slider.value,
                pz = self._z_procs_slider.value,
                nodes = self._nodes.value,
                queue = self._queue.value,
                job_time = self._job_time.value,
            )

            self._status_label.value = "Job submitted!"
            print("Job submitted.")
    
    def _save_config_wrapper(self, caller):
        with self._log_area:
            self.save_config(outfile=self._config_input.value)
        
    def _load_config_wrapper(self, caller):
        with self._log_area:
            print("Load!")
            self.load_config(infile=self._config_input.value)
            
        
    def save_config(self, outfile):
        "Save present state of widget to a file."
        config = dict(
            job_name = self._name.value,
            sheet_dict_list = [
                sheet.generate_sheet_dict() 
                for sheet in self._sheet_widget_area.children
            ],
            nsteps = self._nsteps.value,
            dump_freq = self._dump_freq.value,
            base_dir = self._base_dir.value,
            bounds = list(map(
                list,
                (
                    self._x_bounds_slider.value,
                    self._y_bounds_slider.value,
                    self._z_bounds_slider.value,
                )
            )),                
            px = self._x_procs_slider.value,
            py = self._y_procs_slider.value,
            pz = self._z_procs_slider.value,
            nodes = self._nodes.value,
            queue = self._queue.value,
            job_time = self._job_time.value,
        )
    
        with open(outfile, 'w') as f:
            json.dump(config, f, indent=4)
            
        print("Config saved to {}".format(outfile))

    def load_config(self, infile):
        "Load state of widget from config"
        
        with open(infile) as f:
            config = json.load(f)
        
        self._name.value = config['job_name']
        
        # Remove existing sheet widgets
        for sheet in self._sheet_widget_area.children:
            self._remove_sheet_widget(sheet._remove_button)

        # Create loaded sheet widgets
        for sheet_dict in config['sheet_dict_list']:
            self._add_sheet_widget(config=sheet_dict)

        self._nsteps.value = config['nsteps']
        self._dump_freq.value = config['dump_freq']
        self._base_dir.value = config['base_dir']
        
        self._x_bounds_slider.value = config['bounds'][0]
        self._y_bounds_slider.value = config['bounds'][1]
        self._z_bounds_slider.value = config['bounds'][2]
        
        self._x_procs_slider.value = config['px']
        self._y_procs_slider.value = config['py']
        self._z_procs_slider.value = config['pz']
        self._nodes.value = config['nodes']
        self._queue.value = config['queue']
        self._job_time.value = config['job_time']      
        
        print("Config loaded from {}".format(infile))
