import workflow_objects as kale
import os.path

base_dir = '/home/oliver/lbl/kale'

t = kale.Task('test')

lammps_wf = kale.Workflow('lammps_wf')
lammps_wf.readme = """
<h1>LAMMPS Workflow</h1>
<br>
Simulate molecules from lattice moving from order to disorder
<br>
"""

lammps_task = kale.BatchTask(
    name='lammps_task',
    batch_script=os.path.join(base_dir, "lammps_melt/melt.batch"),
    notebook=os.path.join(base_dir,'LAMMPS Monitor.ipynb'),
    tags=['lammps'],
)
lammps_task.readme = """
Run modified LAMMPS melt example
"""
lammps_wf.add_task(lammps_task)

nb_task = kale.NotebookTask(
    name='analysis_nb',
    notebook=os.path.join(base_dir,'Analysis Notebook.ipynb'),
    tags=['analysis']
)
nb_task.readme = """
Post-simulation analysis notebook
"""
lammps_wf.add_task(nb_task, dependencies=[lammps_task])

