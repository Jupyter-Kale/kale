import workflow_objects as kale

t = kale.Task('test')

example_wf = kale.Workflow('example_wf')
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
out_file = "out.txt"

pwd_task = kale.CommandLineTask(
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
    kale.CommandLineTask(
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


done_task = kale.CommandLineTask(
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

