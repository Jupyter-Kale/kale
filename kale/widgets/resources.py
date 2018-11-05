# stdlib
import socket
import logging

# 3rd party
import numpy as np
import ipywidgets as ipw
import plotly.tools
import plotly.graph_objs


class KaleWorkerResourcesBoard(ipw.VBox):
    def __init__(self, height=-1, width=-1):
        super().__init__()

        self.logger = logging.getLogger()
        self._uheight = height
        self._uwidth = width

        percent_range = [0, 100]

        self._task_cpu_subplots = plotly.tools.make_subplots(
            2, 2,
            print_grid=False,
            subplot_titles=['Host CPU Usage', 'Task CPU Usage', 'Task CPU Cumulative Time', 'Task Thread CPU Usage'])
        self._task_cpu = plotly.graph_objs.FigureWidget(
            self._task_cpu_subplots
        )
        #self._task_cpu['layout']['xaxis1'].update(title='Cores')
        self._task_cpu['layout']['yaxis1'].update(title='% Used', range=percent_range, fixedrange=True)
        #self._task_cpu['layout']['xaxis2'].update(title='Cores')
        self._task_cpu['layout']['yaxis2'].update(title='% Used', range=percent_range, fixedrange=True)
        self._task_cpu['layout']['yaxis3'].update(title='Seconds', type='log')
        self._task_cpu['layout']['yaxis4'].update(title='% Used', range=percent_range, fixedrange=True)
        self._task_cpu.add_traces([
            plotly.graph_objs.Bar(name='Host CPU %'),
            plotly.graph_objs.Bar(name='Task CPU %'),
            plotly.graph_objs.Bar(name='Task CPU Times'),
            plotly.graph_objs.Bar(name='Task Threads User'),
            plotly.graph_objs.Bar(name='Task Threads System')
        ],
        rows=[1,1,2,2,2],
        cols=[1,2,1,2,2])

        self._task_mem_subplots = plotly.tools.make_subplots(
            2, 1,
            print_grid=False,
            subplot_titles=['Host Memory Percent Used', 'Task Memory Bytes Used'])
        self._task_mem = plotly.graph_objs.FigureWidget(self._task_mem_subplots)
        self._task_mem['layout']['yaxis1'].update(title='% Used', range=percent_range, fixedrange=True)
        self._task_mem['layout']['xaxis2'].update(tickangle=-45)
        self._task_mem['layout']['yaxis2'].update(title='Bytes Used', type='log')
        self._task_mem['layout'].update(barmode='overlay')
        self._task_mem.add_traces([
            plotly.graph_objs.Bar(name='Host Memory Used'),
            plotly.graph_objs.Bar(name='Task Memory Used')
        ],
        rows=[1,2],
        cols=[1,1])

        self._task_disk_activity = plotly.graph_objs.FigureWidget(
            [plotly.graph_objs.Heatmap(name='Disk Activity')],
            layout={
                'title': 'Disk Activity'
            }
        )
        self._task_disk_open_files = plotly.graph_objs.FigureWidget(
            [plotly.graph_objs.Table(
                name='Disk I/O: Open Files',
                header={
                    'align': 'center',
                    'values': [
                        'Path',
                        'File_Descriptor',
                        'Offset_Position',
                        'Mode',
                        'Flags'
                        ]
                    })
            ],
            layout={
                'title': 'Open Files'
            }
        )
        self._task_network_activity = plotly.graph_objs.FigureWidget(
            [plotly.graph_objs.Heatmap(name='Network Activity')],
            layout={
                'title': 'Network Activity'
            }
        )
        self._task_network_connections = plotly.graph_objs.FigureWidget(
            [plotly.graph_objs.Table(
                name='Network Connections',
                columnwidth=[120,120,80,120,120,90],
                header={
                    'align': 'center',
                    'values': [
                        'File_Descriptor',
                        'Address_Family',
                        'Address_Type',
                        'Local_Address',
                        'Remote_Address',
                        'Status'
                        ]
                    })
            ],
            layout={
                'title': 'Network I/O: Open Sockets'
            }
        )

        self._host_label = ipw.Label()

        self._task_tabs = ipw.Tab(layout={'height': '480px', 'width': '100%'})
        self._task_tabs.children = [
            ipw.VBox([self._host_label, self._task_cpu]),
            ipw.VBox([self._host_label, self._task_mem]),
            ipw.VBox([self._host_label, self._task_disk_activity, self._task_disk_open_files]),
            ipw.VBox([self._host_label, self._task_network_activity, self._task_network_connections])
            ]
        self._task_tabs.set_title(0, "CPU")
        self._task_tabs.set_title(1, "Memory")
        self._task_tabs.set_title(2, "Disk")
        self._task_tabs.set_title(3, "Network")

        self._task_plots = ipw.VBox([self._task_tabs])

        self.children = [self._task_plots]

    def update(self, data=None):
        try:
            if data is None:
                return

            if 'error' in data:
                print(data['error'])

            if 'host' in data:
                self._host_label.value = 'Host: {}'.format(data['host']['fqdn'])

                with self._task_cpu.batch_update():
                    self._task_cpu.data[0].x = np.arange(len(data['host']['cpu_percent']))
                    self._task_cpu.data[0].y = np.array(data['host']['cpu_percent'])

                with self._task_mem.batch_update():
                    self._task_mem.data[0].x = np.array(['Resident Set Size (RSS)', 'Swap'])
                    self._task_mem.data[0].y = np.array([
                        100.0 - data['host']['percent_available_memory_remaining'],
                        100.0 - data['host']['percent_swap_memory_remaining']
                    ])

                with self._task_disk_activity.batch_update():
                    y = [k for k in data['host']['disk_io_counters']]
                    x = [k for k in data['host']['disk_io_counters'][y[0]]]
                    z = [[v for v in data['host']['disk_io_counters'][k].values()] for k in y]

                    self._task_disk_activity.data[0].x = x
                    self._task_disk_activity.data[0].y = y
                    self._task_disk_activity.data[0].z = z

                with self._task_network_activity.batch_update():
                    y = [k for k in data['host']['net_io_counters']]
                    x = [k for k in data['host']['net_io_counters'][y[0]]]
                    z = [[v for v in data['host']['net_io_counters'][k].values()] for k in y]

                    self._task_network_activity.data[0].x = x
                    self._task_network_activity.data[0].y = y
                    self._task_network_activity.data[0].z = z

                #virtual memory
                #data['host']['virtual_memory']
                #swap memory
                #data['host']['swap_memory']
                # disk i/o
                #data['host']['disk_partitions']
                #data['host']['disk_usage']
                # network nic addresses
                #data['host']['net_if_addrs']
                # network nic stats
                #data['host']['net_if_stats']

            if 'task' in data:
                #print(data['task'])

                network = {
                    "connections": []
                }

                for i in range(len(data['task']['connections'])):
                    family = data['task']['connections'][i]['family']
                    net_type = data['task']['connections'][i]['type']

                    if family == socket.AF_INET:
                        family = "IPv4"
                    elif family == socket.AF_INET6:
                        family = "IPv6"
                    elif family == socket.AF_UNIX:
                        family = "Unix"
                    else:
                        family = "Unrecognized"

                    if net_type == socket.SOCK_STREAM:
                        net_type = "TCP Stream"
                    elif net_type == socket.SOCK_DGRAM:
                        net_type = "UDP Datagram"
                    else:
                        net_type = "Unrecognized"

                    if isinstance(data['task']['connections'][i]['laddr'], list) and \
                            len(data['task']['connections'][i]['laddr']) > 0:
                        local_address = ":".join([
                            "{}".format(x) for x in data['task']['connections'][i]['laddr']
                            ])
                    elif family == "Unix" and \
                            len(data['task']['connections'][i]['laddr']) > 0:
                        local_address = data['task']['connections'][i]['laddr']
                    else:
                        local_address = "N/A"

                    if isinstance(data['task']['connections'][i]['raddr'], list) and \
                            len(data['task']['connections'][i]['raddr']) > 0:
                        remote_address = ":".join([
                            "{}".format(x) for x in data['task']['connections'][i]['raddr']
                            ])
                    elif family == "Unix" and \
                            len(data['task']['connections'][i]['raddr']) > 0:
                        remote_address = data['task']['connections'][i]['raddr']
                    else:
                        remote_address = "N/A"

                    network["connections"].append([
                        data['task']['connections'][i]['fd'],
                        family,
                        net_type,
                        local_address,
                        remote_address,
                        data['task']['connections'][i]['status']
                    ])

                times = None
                if isinstance(data['task']['cpu_times'], list):
                    times = {}
                    keys = data['task']['cpu_times'][0].keys()
                    for i in range(len(data['task']['cpu_times'])):
                        for k in keys:
                            times[k].append(data['task']['cpu_times'][i][k])
                elif isinstance(data['task']['cpu_times'], dict):
                    times = data['task']['cpu_times']
                else:
                    raise Exception("Unexpected format for cpu_times! " +
                        "Expected dict or list of dicts, instead found {}".format(data['task']['cpu_times']))

                with self._task_cpu.batch_update():
                    if isinstance(data['task']['cpu_num'], list):
                        self._task_cpu.data[1].x = np.array([data['task']['cpu_num']])
                    else:
                        self._task_cpu.data[1].x = np.arange(data['task']['cpu_num'])

                    #print("CPU usage {}".format(data['task']['cpu_percent']))
                    self._task_cpu.data[1].y = np.array(data['task']['cpu_percent'])

                    self._task_cpu.data[2].x = np.array([k for k in times.keys()])
                    self._task_cpu.data[2].y = np.array([v for v in times.values()])

                    self._task_cpu.data[3].x = [x['id'] for x in data['task']['threads']]
                    self._task_cpu.data[3].y = [x['user_time'] for x in data['task']['threads']]
                    self._task_cpu.data[4].x = [x['id'] for x in data['task']['threads']]
                    #print([x['system_time'] for x in data['task']['threads']])
                    self._task_cpu.data[4].y = [x['system_time'] for x in data['task']['threads']]

                with self._task_mem.batch_update():
                    self._task_mem.data[1].x = np.array([k for k in data['task']['memory_full_info']])
                    self._task_mem.data[1].y = np.array([v for v in data['task']['memory_full_info'].values()])

                with self._task_disk_open_files.batch_update():
                    # need to transpose the nested list to align the values with columns
                    self._task_disk_open_files.data[0].cells = {
                        'align': 'center',
                        'values': [
                            [x["path"] for x in data['task']['open_files']],
                            [x["fd"] for x in data['task']['open_files']],
                            [x["position"] for x in data['task']['open_files']],
                            [x["mode"] for x in data['task']['open_files']],
                            [x["flags"] for x in data['task']['open_files']]
                        ]
                    }
                    path_col_width = max([len(data['task']['open_files'][i]['path']) * 4
                                          for i in range(len(data['task']['open_files']))])
                    self._task_disk_open_files.data[0].columnwidth = [path_col_width, 50, 50, 40, 40]

                with self._task_network_connections.batch_update():
                    # need to transpose the nested list to align the values with columns
                    self._task_network_connections.data[0].cells = {
                        'align': 'center',
                        'values': [list(x) for x in zip(*network["connections"])]
                    }

                    #k = 0
                    #maxwidth = [x for x in self._task_network_connections.data[0].columnwidth]
                    #for i in range(len(network["connections"])):
                    #    for k in range(len(network["connections"][i])):
                    #        w = len(str(network["connections"][i][k])) * 4
                    #        if len(maxwidth) < k + 1:
                    #            maxwidth.append(w)
                    #        elif w > maxwidth[k]:
                    #            maxwidth[k] = w
                    #self._task_network_connections.data[0].columnwidth = maxwidth
        except Exception as e:
            self.logger.exception(e)
