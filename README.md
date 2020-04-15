# Kale: Human-in-the-loop Interactive Distributed Computing with Jupyter

This is an ongoing research effort at Lawrence Berkeley National Laboratory, aimed at
extending the Jupyter ecosystem with useful add-ons to enable a smooth interactive 
experience for scientific researchers running on clusters, HPC systems etc.

<example notebook visual here>

## Features

### * Control long running remote tasks

- Remotely register any python function that can be pickled as a task
- Start tasks
- Stop tasks
- Change parameters of a Stopped task
- Restart a Stopped task or Start a different task

### * Monitor resource usage

#### -- Host/Node level resource usage

- CPU
- Memory
- Disk
- Network

#### -- Task level resource usage

- CPU
- Memory
- Disk
- Network

## Components

### * Kale Services

- Manager
    - registration/nameserver for workers
    - persistent between tasks

- Worker
    - Wraps task
    - Task registration
    - Task control (start, stop, pause, resume)
    - Resource usage collection

### * Kale Widgets

- Resource Board
    - Host/Node level resource plots
    - Task level resource plots, tables, etc
