# stdlib
import time
import os
import subprocess
import random
import tempfile
import sys

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

def job_running(job_id):
    try:
        subprocess.check_output(['qstat', str(job_id)])
        return True
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
    echo '{randhash}' > {success_file}
    while [ -z $(grep -Fx '{randhash}' '{success_file}') ]
    do
        sleep 1
    done
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
    with open(script_path) as fh:
        print(fh.read())
    command = ['qsub', script_path]
    print("QSUB COMMAND: {}".format(' '.join(command)))
    return subprocess.check_output(command).decode().strip()

def poll_success_file(filepath, job_id, randhash, poll_interval):
    try:
        while job_running(job_id):
            time.sleep(poll_interval)

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
    print(success_file)
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
#    if type(nodes_cores) is dict:
#        node_string = '+'.join([
#            '{node}:ppn={cores}'.format(node=node,cores=cores)
#            for node,cores in nodes_cores.items()
#        ])
#    elif type(nodes_cores) in (int, str):
#        node_string = '1:ppn={}'.format(nodes_cores)
#    else:
#        raise ValueError("Received unexpected type for nodes_cores in get_nodes_string.")
#    if node_property is not None:
#        node_string += ':{}'.format(node_property)

    node_string = '{}'.format(nodes_cores)

    return node_string


# TODO - review
def run_cmd_job(command, name, nodes_cores, node_property=None, poll_interval=60, mpiexec="/opt/open-mpi/ib-gnu44/bin/mpiexec"):
    tmp_batch_script = get_tempfile()
    batch_string = """#!/bin/bash
#PBS -l nodes={nodes_string}
#PBS -l nice=10
#PBS -j oe
#PBS -q default
#PBS -N {name}
#PBS -r n
cd $PBS_O_WORKDIR
{command}""".format(
#{mpiexec} {command}""".format(
        command=command,
        name=name,
        #mpiexec=mpiexec,
        nodes_string=get_nodes_string(nodes_cores, node_property)
    )

    #print("Run cmd job.")
    #print(batch_string)

    with open(tmp_batch_script, 'w') as fh:
        fh.write(batch_string)

    run_batch_job(tmp_batch_script, poll_interval)
