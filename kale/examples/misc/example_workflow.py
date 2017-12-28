import kale.workflow_objects

##########
## BASH ##
##########

example_wf = kale.workflow_objects.Workflow('example_wf')
example_wf.readme = """
<h1>Example Workflow</h1>
<br>
Very simple map-reduce-like workflow, which involves:
<br>
<ol>
    <li>Short intro task</li>
    <li>Long parallel tasks</li>
    <li>Short outro tasks</li>
</ol>

<br>
The final results are printed to 'out.txt'.
"""

n_echos = 10
n_tasks = 3
out_file = "/tmp/scratch/out.txt"

pwd_task = kale.workflow_objects.CommandLineTask(
    name='pwd_task',
    command="""echo "Hello from '`pwd`'." > {out_file}""",
    output_files=["{out_file}"],
    log_path="{out_file}",
    tags=['pwd'],
    params=dict(out_file=out_file)
)
pwd_task.readme = """
<h3>PWD Task</h3>
<br>
This task prints the current working directory.
"""

echo_tasks = [
    kale.workflow_objects.CommandLineTask(
        name='echo_task_{i}',
        command='for j in {{1..{n_echos}}}; do echo "Hello #{i}: $j" >> {out_file}; sleep 1; done',
        output_files=["{out_file}"],
        log_path="{out_file}",
        tags=['echo'],
        params=dict(out_file=out_file, i=i, n_echos=n_echos)
    )
    for i in range(n_tasks)
]

for i,task in enumerate(echo_tasks):
    task.readme = """
    <h3>Echo Task #{i}</h3>
    <br>
    Do the following:
    <ul>
        <li>Print 'Hello #{i}'</li>
        <li>Sleep for 1 second</li>
        <li>Repeat</li>
    </ul>
    And be happy! :)
    """.format(i=i)


done_task = kale.workflow_objects.CommandLineTask(
    name='done_task',
    command="echo 'Done!' >> {out_file}",
    output_files=["{out_file}"],
    log_path="{out_file}",
    tags=['done'],
    params=dict(out_file=out_file)
)

done_task.readme = """
<h3>Done Task</h3>
<br>
Just print 'Done!' :)
"""

example_wf.add_task(pwd_task)
for i in range(n_tasks):
    example_wf.add_task(echo_tasks[i], dependencies=[pwd_task])
example_wf.add_task(done_task, dependencies=echo_tasks)

############
## PYTHON ##
############
example_wf_py = kale.workflow_objects.Workflow('example_wf_py_py')
example_wf_py.readme = """
<h1>Example Workflow</h1>
<br>
Very simple map-reduce-like workflow, which involves:
<br>
<ol>
    <li>Short intro task</li>
    <li>Long parallel tasks</li>
    <li>Short outro tasks</li>
</ol>

<br>
The final results are printed to 'out.txt'.
"""

n_echos = 10
n_tasks = 3
out_file = "/tmp/scratch/out.txt"

def pwd_func():
    import os
    import time
    print ("Hello from {}.".format(os.getcwd()))
    time.sleep(1)

pwd_task_py = kale.workflow_objects.PythonFunctionTask(
    name='pwd_task',
    func=pwd_func,
    output_files=["{out_file}"],
    log_path="{out_file}",
    tags=['pwd'],
    params=dict(out_file=out_file)
)
pwd_task_py.readme = """
<h3>PWD Task</h3>
<br>
This task prints the current working directory.
"""

def echo_func(n_echos):
    import time
    for j in range(n_echos):
        print("Hello #{}".format(j))
        time.sleep(1)

echo_tasks_py = [
    kale.workflow_objects.PythonFunctionTask(
        name='echo_task_{i}',
        func=echo_func,
        output_files=["{out_file}"],
        log_path="{out_file}",
        tags=['echo'],
        args=[n_echos],
        params=dict(out_file=out_file, i=i, n_echos=n_echos)
    )
    for i in range(n_tasks)
]

for i,task in enumerate(echo_tasks_py):
    task.readme = """
    <h3>Echo Task #{i}</h3>
    <br>
    Do the following:
    <ul>
        <li>Print 'Hello #{i}'</li>
        <li>Sleep for 1 second</li>
        <li>Repeat</li>
    </ul>
    And be happy! :)
    """.format(i=i)

def done_func():
    print("Done!")

done_task_py = kale.workflow_objects.PythonFunctionTask(
    name='done_task',
    func=done_func,
    output_files=["{out_file}"],
    log_path="{out_file}",
    tags=['done'],
    params=dict(out_file=out_file)
)

done_task_py.readme = """
<h3>Done Task</h3>
<br>
Just print 'Done!' :)
"""

example_wf_py.add_task(pwd_task_py)
for i in range(n_tasks):
    example_wf_py.add_task(echo_tasks_py[i], dependencies=[pwd_task_py])
example_wf_py.add_task(done_task_py, dependencies=echo_tasks_py)
