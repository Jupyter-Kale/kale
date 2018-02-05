# stdlib
import time
import os
import sys
import subprocess
import random

# 3rd party
import tempfile

def determine_batch_manager():
    options = {
        'torque': 'qsub',
        'slurm': 'sbatch'
    }

    for manager, command in options.items():
        # Returns 0 if option exists, 1 otherwise
        if not subprocess.call(['which', command]):
            return manager

    raise ValueError("Could not locate batch manager.")

def wait_for_fifo(path, key):
    "Wait for key to be written to path. Pass if correct, fail otherwise."
    with open(path) as fh:
        if fh.read() == key:
            pass
        else:
            sys.exit(1)

def gen_fifo():
    fname = subprocess.getoutput('mktemp -u -p . -t .watch_nb.XXXXXXXXXX').strip()
    subprocess.call(['mkfifo',fname])
    return fname

def gen_random_hash():
    return "%032x" % random.getrandbits(128)

def job_running(job_id, check_cmd):
    try:
        print(*[check_cmd, str(job_id)])
        out = subprocess.check_output(
            "{} {}".format(check_cmd, str(job_id)),
            shell=True
        ).decode().strip()
        print('out = "{}"'.format(out))
        if len(out) > 0:
            return True
        else:
            return False
    except subprocess.CalledProcessError:
        return False

def get_tempfile():
    return subprocess.check_output('mktemp').decode().strip()

def create_appended_text_file(path, addition):
    fname = os.path.basename(path)
    newpath = get_tempfile()
    
    with open(path) as fh:
        content = fh.read()
        
    with open(newpath, 'w') as fh:
        fh.write(content)
        fh.write('\n')
        fh.write(addition)
        
    return newpath

def create_success_file():
    fd, path = tempfile.mkstemp(prefix='.watch_job_', dir='.')
    os.close(fd)
    return path

def wrap_batch_script(batch_script, success_file, randhash):
    success_command = """
    stdbuf -o0 -e0 echo '{randhash}' > {success_file}
    #while [ -z $(grep -Fx '{randhash}' '{success_file}') ]
    #do
    #    sleep 1
    #done
    """.format(
        randhash=randhash,
        success_file=success_file
    )

    new_batch_script = create_appended_text_file(
        batch_script,
        success_command
    )

    return new_batch_script

def submit_batch_script(script_path):
    """Submit job, decode job_id bytes & remove newline
    Holds are passed to qsub via -h.
    """
    #with open(script_path) as fh:
    #    print(fh.read())

    batch_manager = determine_batch_manager()
    if batch_manager == 'torque':
        sub_cmd = 'qsub'
    elif batch_manager == 'slurm':
        sub_cmd = 'sbatch --parsable'

    command = ' '.join([sub_cmd, script_path])
    print("QSUB COMMAND: {}".format(command))
    return subprocess.check_output(command, shell=True).decode().strip()

def poll_success_file(filepath, job_id, randhash, poll_interval):
    batch_manager = determine_batch_manager()
    if batch_manager == 'torque':
        check_cmd = 'qstat'
    elif batch_manager == 'slurm':
        check_cmd = 'squeue -h --job'

    try:
        #print("Before while")
        while job_running(job_id, check_cmd):
            time.sleep(poll_interval)
        #print("After while")

        # Job is no longer in batch queue
        try:
            with open(filepath) as fh:
                message = fh.read().strip()
            if message == randhash:
                print("Job success.")
            elif message == '':
                print("Job failed.")
                #sys.exit(1)
            else:
                print("Wrong hash!")
                print("Wanted '{}'".format(randhash))
                print("Received '{}'".format(message))
                #sys.exit(1)
        except FileNotFoundError:
            # No success message means job failed
            print("Unexpected error.")
            #sys.exit(1)
    finally:
        # Always delete success file
        os.remove(filepath)

def run_batch_job(batch_script, node_property=None, poll_interval=60):
    print("Run batch job")
    success_file = create_success_file()
    #print(success_file)
    randhash = gen_random_hash()
    
    new_batch_script = wrap_batch_script(
        batch_script, 
        success_file,
        randhash
    )
    
    job_id = submit_batch_script(new_batch_script)
    
    poll_success_file(success_file, job_id, randhash, poll_interval)

def get_nodes_string(nodes_cores, node_property):
    """
    nodes_cores is a dict with nodes as keys, num_cores as items.
    Alternatively, it can be an int with # of cores.
    """

    batch_manager = determine_batch_manager()
    if batch_manager == 'torque':
        if type(nodes_cores) is dict:
            node_string = '+'.join([
                '{node}:ppn={cores}'.format(node=node,cores=cores)
                for node,cores in nodes_cores.items()
            ])
        elif type(nodes_cores) in (int, str):
            node_string = '1:ppn={}'.format(nodes_cores)
        else:
            raise ValueError("Received unexpected type for nodes_cores in get_nodes_string.")
        if node_property is not None:
            node_string += ':{}'.format(node_property)

    # DEFINITELY COULD BE IMPROVED
    elif batch_manager == 'slurm':
        if type(nodes_cores) is dict:
            node_string = str(len(nodes_cores.keys()))
        elif type(nodes_cores) in (int, str):
            node_string = '1'

    return node_string


# TODO - review
def run_cmd_job(command, name, nodes_cores, time='10:00', node_property=None, poll_interval=60, mpiexec="/opt/open-mpi/ib-gnu44/bin/mpiexec"):
    tmp_batch_script = get_tempfile()
    batch_manager = determine_batch_manager()
    if batch_manager == 'torque':
        batch_template = """#!/bin/bash
#PBS -l nodes={nodes_string}
#PBS -l nice=10
#PBS -j oe
#PBS -q default
#PBS -N {name}
#PBS -r n
cd $PBS_O_WORKDIR
{command}"""
    elif batch_manager == 'slurm':
        batch_template = """#!/bin/bash
#SBATCH -J {name}
#SBATCH -p debug
#SBATCH -N {nodes_string}
#SBATCH -t {time}
#SBATCH -o {name}.%j
#SBATCH -C haswell
#SBATCH -L SCRATCH
{command}"""

    batch_string = batch_template.format(
        #{mpiexec} {command}""".format(
        command=command,
        name=name,
        time=time,
        #mpiexec=mpiexec,
        nodes_string=get_nodes_string(nodes_cores, node_property)
    )

    #print("Run cmd job.")
    #print(batch_string)

    with open(tmp_batch_script, 'w') as fh:
        fh.write(batch_string)

    run_batch_job(tmp_batch_script, poll_interval)
