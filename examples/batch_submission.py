import zmq
import time
import os
from concurrent.futures import ThreadPoolExecutor
import subprocess
import ipywidgets as ipw
import random
import sys

def gen_random_hash():
    return "%032x" % random.getrandbits(128)

def job_running(job_id):
    try:
        subprocess.check_output(['qstat', str(job_id)])
        return True
    except subprocess.CalledProcessError:
        return False

def tempfile():
    return subprocess.check_output('mktemp').decode().strip()

def create_appended_text_file(path, addition):
    fname = os.path.basename(path)
    newpath = tempfile()
    
    with open(path) as fh:
        content = fh.read()
        
    with open(newpath, 'w') as fh:
        fh.write(content)
        fh.write('\n')
        fh.write(addition)
        
    return newpath

def open_success_port():
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    port = socket.bind_to_random_port("tcp://*")
    return socket, port

def get_hostname():
    return subprocess.check_output(['hostname']).decode().strip()

def wrap_batch_script(batch_script, success_port, randhash):
    success_uri = "tcp://{hostname}:{port}".format(
        hostname=get_hostname(),
        port=success_port
    )
    
    success_func = r"""import zmq
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect('{success_uri}')
socket.send(b'{randhash}')""".format(
        success_uri=success_uri,
        randhash=randhash
    ).replace('\n','; \\\n')
    
    success_command = 'python -c "{}"'.format(
        success_func
    )
    
    new_batch_script = create_appended_text_file(
        batch_script, 
        success_command
    )
    
    return new_batch_script

def submit_batch_script(script_path):
    "Submit job, decode job_id bytes & remove newline"
    with open(script_path) as fh:
        print(fh.read())
    return subprocess.check_output(['qsub', script_path]).decode().strip()

def listen_for_success(socket, job_id, randhash, delay):
    while job_running(job_id):
        time.sleep(delay)
    
    # Job is no longer in batch queue
    try:
        message = socket.recv(zmq.NOBLOCK).decode().strip()
        if message == randhash:
            print("Job success.")
        else:
            print("Wrong hash!")
            print("Wanted '{}'".format(randhash))
            print("Received '{}'".format(message))
            sys.exit(1)
    except zmq.Again:
        # No success message means job failed
        print("Job failed.")
        sys.exit(1)

def run_batch_job(batch_script, poll_delay=5):
    socket, success_port = open_success_port()
    
    randhash = gen_random_hash()
    
    new_batch_script = wrap_batch_script(
        batch_script, 
        success_port,
        randhash
    )
    
    job_id = submit_batch_script(new_batch_script)
    
    listen_for_success(socket, job_id, randhash, poll_delay)

if  __name__ == '__main__':
    run_batch_job('/home/oge1/kale/batch_scripts/test.batch')

