from workflow_widgets import WorkflowWidget, WorkerPoolWidget, TailWidget
from droplet_workflow import droplet_wf
from example_workflow import example_wf
from aux_widgets import QueueWidget, NEWTAuthWidget, SSHAuthWidget, SSHTerminal, Space
import workflow_objects as kale

import ipywidgets as ipw
import time
from concurrent.futures import ThreadPoolExecutor
import traitlets
import networkx as nx
import IPython.display as disp

def test_tag_to_selection():
    wpw = WorkerPoolWidget()
    ww = WorkflowWidget(workflow=droplet_wf, worker_pool_widget=wpw)

    ww._tag_selector.index = 5 # parse
    ww._ta_tag_to_selection()
    ww._ta_append_all_children()
    ww._tag_selector.index = 12 # 100A
    ww._ta_selection_to_tag()
    s1 = ww.bqgraph.selected

    ww._ta_select_none()
    ww._ta_tag_to_selection()
    s2 = ww.bqgraph.selected

    d1 = set(s1).difference(s2)
    d2 = set(s2).difference(s1)

    if len(d1.union(d2)) == 0:
        print("Woohoo!")

    else:
        print("Fail :(")
        print("s1 - s2 = {}".format(sorted(list(d1))))
        print("s2 - s1 = {}".format(sorted(list(d2))))
        raise ValueError("Fail")
