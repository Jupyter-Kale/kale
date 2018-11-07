# stdlib
import errno
import logging
import logging.handlers
import json
import multiprocessing
import os
import pickle
import socket
import sys
import time
import traceback
import uuid

# 3rd party
import psutil
import requests
import sanic
import sanic.response
import sanic.exceptions

# local
from . import db
from . import manager

mp = multiprocessing.get_context('spawn')

_RLIMIT_CONSTANTS = {k: v for k, v in psutil.__dict__.items() if k.startswith("RLIMIT")}

def get_kale_id():
    return str(uuid.uuid4())


def spawn_worker(kale_id, whost="127.0.0.1", mhost="127.0.0.1", mport=8099):
    _logger = logging.getLogger(__name__)
    _logger.debug("spawn_worker")
    app = KaleWorker(kale_id, mhost, mport)
    p = mp.Process(target=app.run, args=[whost])
    p.start()
    return p


def run_function(f=None, args=(), kwargs=None, whost="127.0.0.1", mhost="127.0.0.1", mport=8099):
    mgr = manager.KaleManagerClient(host=mhost,port=mport)
    kale_id = get_kale_id()
    kale_proc = spawn_worker(kale_id, whost=whost, mhost=mhost, mport=mport)

    kale_task = None
    kale_worker = None

    try:
        while 1:
            try:
                worker_info = mgr.get_worker(kale_id)
                break
            except requests.HTTPError:
                time.sleep(0.1)

        kale_worker = KaleWorkerClient(worker_info["host"], worker_info["port"])

        # make sure the service is listening
        while 1:
            try:
                kale_worker.get_service_status()
                break
            except requests.ConnectionError:
                time.sleep(0.1)

        kale_task = kale_worker.register_function_task(f, args, kwargs)
        kale_worker.start_task(kale_task)

        while kale_worker.get_task_status(kale_task) != "running":
            time.sleep(1)

        while 1:
            try:
                results = kale_worker.get_task_output(kale_task)
                error = None
                break
            except requests.HTTPError:
                time.sleep(1)
    except Exception as e:
        results = None
        error = e
    finally:
        if kale_task is not None and kale_worker is not None:
            kale_worker.stop_task(kale_task)
        if kale_worker is not None:
            kale_worker.shutdown()
        kale_proc.join()

    if error is None:
        return results
    else:
        raise Exception(error)


async def run_async_function(f=None, args=(), kwargs=None, whost="127.0.0.1", mhost="127.0.0.1", mport=8099):
    mgr = manager.KaleManagerClient(host=mhost, port=mport)
    kale_id = get_kale_id()
    kale_proc = spawn_worker(kale_id, whost=whost, mhost=mhost, mport=mport)

    kale_task = None
    kale_worker = None

    try:
        while 1:
            try:
                worker_info = mgr.get_worker(kale_id)
                break
            except requests.HTTPError:
                time.sleep(0.1)

        kale_worker = KaleWorkerClient(worker_info["host"], worker_info["port"])

        # make sure the service is listening
        while 1:
            try:
                kale_worker.get_service_status()
                break
            except requests.ConnectionError:
                time.sleep(0.1)

        kale_task = kale_worker.register_function_task(f, args, kwargs)
        kale_worker.start_task(kale_task)

        while kale_worker.get_task_status(kale_task) != "running":
            time.sleep(1)

        while 1:
            try:
                results = kale_worker.get_task_output(kale_task)
                error = None
                break
            except requests.HTTPError:
                time.sleep(1)
    except Exception as e:
        results = None
        error = e
    finally:
        if kale_task is not None and kale_worker is not None:
            kale_worker.stop_task(kale_task)
        if kale_worker is not None:
            kale_worker.shutdown()
        kale_proc.join()

    if error is None:
        return results
    else:
        raise Exception(error)



def kill_process_tree(pid, timeout=3):
    if pid == os.getpid():
        raise RuntimeWarning("Process {} attempted to kill itself!".format(pid))

    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for p in children:
        p.terminate()
    gone, alive = psutil.wait_procs(children, timeout=timeout)
    for p in alive:
        p.kill()

    return True


class KaleWorker(sanic.Sanic):
    def __init__(self, kale_id=None, mhost="127.0.0.1", mport=8099):
        super().__init__()
        assert kale_id is not None, "kale_id must be a valid identifier"
        self._kale_id = kale_id
        self._manager = (mhost,mport)
        self._manager_url = "http://{}:{}".format(mhost,mport)
        self._task_manager = None
        self.logger = None
        self.add_route(self.serve_task_status, "/task/<task_id>/status", methods=["GET"])
        self.add_route(self.serve_tasks, "/task", methods=["GET"])
        self.add_route(self.serve_register, "/task", methods=["POST"])
        self.add_route(self.serve_start, "/task/<task_id>/start", methods=["POST"])
        self.add_route(self.serve_stop, "/task/<task_id>/stop", methods=["POST"])
        self.add_route(self.serve_suspend, "/task/<task_id>/suspend", methods=["POST"])
        self.add_route(self.serve_resume, "/task/<task_id>/resume", methods=["POST"])
        self.add_route(self.serve_resources, "/task/<task_id>/resources", methods=["GET"])
        self.add_route(self.serve_results, "/task/<task_id>/results", methods=["GET"])
        self.add_route(self.serve_shutdown, "/shutdown", methods=["POST"])
        self.add_route(self.serve_service_status, "/", methods=["GET"])

    def register_worker(self, kale_id, host, port):
        # determine ip address that routes to manager service, in case of 0.0.0.0 or unresolved DNS names
        # also ensures that the manager service is reachable from the worker
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.settimeout(3)
            s.connect(self._manager)
            _host = s.getsockname()[0]
        except socket.timeout as e:
            self.logger.exception(e)
            raise IOError("Worker {} timed out trying to connect via socket to manager service at {}!".format(
                kale_id, self._manager_url))
        except Exception as e:
            self.logger.exception(e)
            raise
        finally:
            s.shutdown(socket.SHUT_RDWR)
            s.close()

        self.logger.debug("register_worker {} {} {}".format(kale_id, _host, port))
        response = requests.post("{}/worker".format(self._manager_url),
                                 data=json.dumps({"id": kale_id, "host": _host, "protocol": "http", "port": port}))
        return response.json()

    def unregister_worker(self):
        self.logger.debug("unregister worker {}".format(self._kale_id))
        response = requests.delete("{}/worker/{}".format(self._manager_url, self._kale_id))
        self.logger.debug(response)

        try:
            response.json()
        except json.decoder.JSONDecodeError:
            self.logger.debug(response.text)
            raise Exception("Did not receive proper JSON response from unregister: {}".format(response.text))

        return response.json()

    def run(self, host="127.0.0.1", port=None, debug=False, ssl=None,
            sock=None, workers=1, protocol=None,
            backlog=100, stop_event=None, register_sys_signals=True,
            access_log=True):
        self.logger = logging.getLogger("KaleWorker {}".format(self._kale_id))
        #self.logger.setLevel(logging.DEBUG)
        #self.logger.addHandler(logging.handlers.RotatingFileHandler("/tmp/scratch/KaleWorker_{}".format(
        #    self._kale_id.replace("-","_")
        #    )))

        _port = port

        if _port is None:
            # IANA unassigned port range is 49152 - 65535
            _port = 49152

        # make sure we have an available port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        port_selected = False

        while not port_selected:
            if _port > 65535:
                raise OSError("Unable to find any open ports!")

            try:
                s.bind((host, _port))
                port_selected = True
            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    _port = _port + 1
                else:
                    raise

        self.logger.debug("run {} {} {}".format(self._kale_id, host, _port))
        self._task_manager = KaleTaskManager(self._kale_id)
        self.register_worker(self._kale_id, host, _port)
        # restrict service to one process
        return super(KaleWorker, self).run(None, None, debug, ssl, s, 1, protocol, backlog,
                                            stop_event, register_sys_signals, access_log)

    def get_service_status(self):
        return psutil.Process().status()

    def shutdown_service(self, delay=5):
        self._task_manager.shutdown()

        # reap any zombie processes
        _children = mp.active_children()
        for c in _children:
            if c.is_alive():
                c.join(3)

        for c in _children:
            if c.is_alive():
                kill_process_tree(c.pid)

        self.unregister_worker()
        self.loop.call_later(delay, self.stop)
        return True

    def serve_service_status(self, request):
        status = self.get_service_status()
        return sanic.response.json({"status": status})

    def serve_tasks(self, request):
        tasks = self._task_manager.get_tasks()
        _tasks = {}
        for t in tasks:
            _tasks[t[0]] = {
                "id": t[0],
                "name": t[-2],
                "pid": t[-1]
            }
        return sanic.response.json({"tasks": _tasks})

    def serve_task_status(self, request, task_id=None):
        try:
            status = self._task_manager.get_task_status(task_id)
            return sanic.response.json({"status": status})
        except psutil.NoSuchProcess as e:
            return sanic.response.json({"status": psutil.STATUS_DEAD})

    def serve_register(self, request):
        self.logger.debug("serve_register")
        task_id = self._task_manager.register_task(
            bytes(request.json["target"]),
            request.json["call"],
            bytes(request.json["args"]),
            bytes(request.json["kwargs"]),
            request.json["task_name"])
        return sanic.response.json({"id": task_id})

    def serve_start(self, request, task_id):
        self.logger.debug("serve_start")
        try:
            pid = self._task_manager.start_task(task_id)
            return sanic.response.json({"pid": pid})
        except Exception as e:
            return sanic.response.json({"error": "{} failed to start {}".format(
                task_id, traceback.format_exception(etype=e.__class__, value=e, tb=e.__traceback__))})

    def serve_stop(self, request, task_id):
        try:
            self._task_manager.stop_task(task_id)
            return sanic.response.json({"status": "{} stopped".format(task_id)})
        except Exception as e:
            return sanic.response.json({"status": "{} error: {}".format(task_id, e.args)})

    def serve_suspend(self, request, task_id):
        try:
            self._task_manager.suspend_task(task_id)
            return sanic.response.json({"status": "{} suspended".format(task_id)})
        except Exception as e:
            return sanic.response.json({"status": "{} error: {}".format(task_id, e.args)})

    def serve_resume(self, request, task_id):
        try:
            self._task_manager.resume_task(task_id)
            return sanic.response.json({"status": "{} resumed".format(task_id)})
        except Exception as e:
            return sanic.response.json({"status": "{} error: {}".format(task_id, e.args)})

    def serve_resources(self, request, task_id):
        resources = self._task_manager.get_task_resources(task_id)
        return sanic.response.json(resources)

    def serve_results(self, request, task_id):
        try:
            results = self._task_manager.get_task_results(task_id)
            return sanic.response.json({"results": list(pickle.dumps(results, protocol=pickle.HIGHEST_PROTOCOL))})
        except IOError as e:
            return sanic.response.json({"error": "{}".format(e.args)}, status=404)
        except Exception as e:
            return sanic.response.json({"error": "{}".format(e.args)}, status=500)

    def serve_shutdown(self, request):
        delay = 5
        self.shutdown_service()
        return sanic.response.json({"status": "Shutting down in {} seconds!".format(delay)})


class KaleTaskManager(object):
    def __init__(self, kale_id=None, logger=None):
        self.tasks = db.TaskStore()
        self._tasks = {}

        assert kale_id is not None, "kale_id is required"

        if logger is None:
            self.logger = logging.getLogger('KaleTaskManager - {}'.format(kale_id))
        else:
            self.logger = logger

    def get_tasks(self):
        self.logger.debug("get_tasks")
        return self.tasks.list()

    def get_task_status(self, task_id):
        self.logger.debug("get_task_status")
        pid = self.tasks.find(task_id)[-1]

        if pid == -1:
            return "not running"

        if self._tasks[task_id]["results"] is None:
            return psutil.Process(pid).status()
        else:
            status = psutil.Process(pid=pid).status()
            if status == psutil.STATUS_RUNNING:
                return "completed"
            else:
                return "results available, {}".format(status)

    def register_task(self, target, call, args, kwargs, task_name):
        self.logger.debug("register_task")
        task_id = self.tasks.add(target, call, args, kwargs, task_name)
        return task_id

    def start_task(self, task_id):
        self.logger.debug("start_task")
        # pull the task DB info, unpack data into what the task needs
        row = self.tasks.find(task_id)
        target = pickle.loads(row[1])
        call = row[2]
        assert callable(getattr(target, call))
        args = pickle.loads(row[3])
        kwargs = pickle.loads(row[4])
        name = row[5]
        # set up the connection to receive task results
        worker_conn, task_conn = mp.Pipe(duplex=False)

        # make sure default signal handlers exist for suspend and resume
        #sigstop = signal.signal(signal.SIGSTOP, signal.SIG_DFL)
        #sigcont = signal.signal(signal.SIGCONT, signal.SIG_DFL)

        # create the task, start it, release the task connection end
        p = KaleTask(target=getattr(target, call), name=name, args=args, kwargs=kwargs, results_pipe=task_conn)
        p.start()

        task_conn.close()
        # save state
        self.tasks.update_pid(task_id, p.pid)
        self._tasks[task_id] = {}
        self._tasks[task_id]["process"] = p
        self._tasks[task_id]["results_pipe"] = worker_conn
        self._tasks[task_id]["results"] = None
        return p.pid

    def stop_task(self, task_id):
        f = open('/tmp/scratch/task_{}'.format(task_id), 'w')
        self.logger.debug("stop_task")
        f.write("stop_task\n")
        f.flush()

        pid = self.tasks.find(task_id)[-1]
        try:
            self._tasks[task_id]["results_pipe"].close()
            self._tasks[task_id]["results_pipe"] = None
        except Exception:
            pass

        self.logger.debug("task pid {}".format(pid))
        self.logger.debug("checking children for specific task pid")

        f.write("task pid {}\n".format(pid))
        f.write("checking children for specific task pid\n")
        f.flush()

        child_procs = psutil.Process().children()

        f.write("number of child processes: {}\n".format(len(child_procs)))

        for p in child_procs:
            self.logger.debug("child pid {}, target pid {}".format(p.pid, pid))
            f.write("child pid {}, target pid {}\n".format(p.pid, pid))
            f.write("{} {}\n".format(p.pid == pid, p.is_running()))
            f.flush()
            if p.pid == pid and p.is_running():
                resources = {}
                try:
                    resources["num_fds"] = p.num_fds()
                    resources["num_threads"] = p.num_threads()
                    resources["open_files"] = p.open_files()
                    resources["connections"] = p.connections()
                except:
                    pass

                self.logger.debug("child task found")
                self.logger.debug("child task resources -- {}".format(resources))

                f.write("child task found\n")
                f.write("child task resources -- {}\n".format(resources))
                f.flush()

                self.logger.debug("terminate all children of this task")
                f.write("terminate all children of this task\n")
                f.flush()
                if len(p.children()) > 0:
                    kill_process_tree(pid)

                self.logger.debug("wait up to 3 seconds for this child task to end")
                f.write("wait up to 3 seconds for this child task to end\n")
                f.flush()
                self._tasks[task_id]["process"].join(3)

                if p.is_running():
                    self.logger.debug("child task was not terminated yet, sending kill signal")
                    f.write("child task was not terminated yet, sending kill signal\n")
                    f.flush()
                    p.kill()

                break
        else:
            self.logger.warning("task: {} was not running".format(task_id))
            f.write("task: {} was not running\n".format(task_id))
            f.flush()

        f.close()
        self.tasks.update_pid(task_id, -1)
        return True

    def suspend_task(self, task_id):
        pid = self._tasks[task_id].find(task_id)[-1]
        if pid == -1:
            raise psutil.NoSuchProcess("task: {} was not running".format(task_id))
        else:
            psutil.Process(pid).suspend()
            return True

    def resume_task(self, task_id):
        pid = self._tasks[task_id].find(task_id)[-1]
        if pid == -1:
            raise psutil.NoSuchProcess("task: {} was not running".format(task_id))
        else:
            psutil.Process(pid).resume()
            return True

    def get_task_resources(self, task_id):
        self.logger.debug("get_task_resources")
        pid = self.tasks.find(task_id)[-1]

        if pid == -1:
            raise psutil.NoSuchProcess("task: {} was not running".format(task_id))

        task = psutil.Process(pid)

        try:
            task_usage = {}
            with task.oneshot():
                task_usage["pid"] = task.pid
                task_usage["ppid"] = task.ppid()
                task_usage["name"] = task.name()
                task_usage["executable"] = task.exe()
                task_usage["cmdline"] = task.cmdline()
                task_usage["environ"] = task.environ()
                task_usage["create_time"] = task.create_time()
                task_usage["status"] = task.status()
                task_usage["cwd"] = task.cwd()
                task_usage["username"] = task.username()
                task_usage["uids"] = task.uids()
                task_usage["gids"] = task.gids()
                task_usage["terminal"] = task.terminal()
                task_usage["nice"] = task.nice()
                task_usage["ionice"] = task.ionice()._asdict()
                task_usage["rlimits"] = {k: task.rlimit(v) for k, v in _RLIMIT_CONSTANTS.items()}
                task_usage["io_counters"] = task.io_counters()._asdict()
                task_usage["num_ctx_switches"] = task.num_ctx_switches()
                task_usage["num_fds"] = task.num_fds()
                task_usage["num_threads"] = task.num_threads()
                task_usage["threads"] = [x._asdict() for x in task.threads()]
                task_usage["cpu_percent"] = task.cpu_percent(interval=0.1)
                task_usage["cpu_times"] = task.cpu_times()._asdict()
                task_usage["cpu_affinity"] = task.cpu_affinity()
                task_usage["cpu_num"] = task.cpu_num()
                task_usage["memory_full_info"] = task.memory_full_info()._asdict()
                task_usage["memory_percent"] = {k: task.memory_percent(k) for k in task_usage["memory_full_info"]}
                task_usage["memory_maps"] = [x._asdict() for x in task.memory_maps()]
                task_usage["open_files"] = [x._asdict() for x in task.open_files()]
                task_usage["connections"] = [x._asdict() for x in task.connections()]

            swap_mem = psutil.swap_memory()
            virtual_mem = psutil.virtual_memory()
            partitions = psutil.disk_partitions(all=True)
            net_if_addrs = psutil.net_if_addrs()
            hostname = socket.gethostname()

            if swap_mem[1] < 1E-6:
                pct_swap_rem = 100.0
            else:
                pct_swap_rem = swap_mem[1]/swap_mem[0] * 100.0
            
            if virtual_mem[1] < 1E-6:
                pct_avail_rem = 100.0
            else:
                pct_avail_rem = virtual_mem[1]/virtual_mem[0] * 100.0

            disk_usage = {}
            for i in range(len(partitions)):
                mount = partitions[i].mountpoint
                try:
                    disk_usage[mount] = psutil.disk_usage(mount)._asdict()
                except PermissionError as e:
                    continue

            data = {
                "host": {
                    "hostname": hostname,
                    "fqdn": socket.getfqdn(hostname),
                    "cpu_percent": psutil.cpu_percent(percpu=True),
                    "cpu_times": [x._asdict() for x in psutil.cpu_times(percpu=True)],
                    "cpu_times_percent": [x._asdict() for x in psutil.cpu_times_percent(percpu=True)],
                    "cpu_stats": psutil.cpu_stats()._asdict(),
                    "cpu_freq": [x._asdict() for x in psutil.cpu_freq(percpu=True)],
                    "cpu_count": {
                        "physical": psutil.cpu_count(logical=False),
                        "logical": psutil.cpu_count()
                    },
                    "percent_swap_memory_remaining": pct_swap_rem,
                    "percent_available_memory_remaining": pct_avail_rem,
                    "swap_memory": swap_mem._asdict(),
                    "virtual_memory": virtual_mem._asdict(),
                    "disk_partitions": [x._asdict() for x in partitions],
                    "disk_usage": disk_usage,
                    "disk_io_counters": {k: v._asdict() for k,v in psutil.disk_io_counters(perdisk=True).items()},
                    "net_io_counters": {k: v._asdict() for k,v in psutil.net_io_counters(pernic=True).items()},
                    "net_if_addrs": {k: [x._asdict() for x in net_if_addrs[k]] for k in net_if_addrs},
                    "net_if_stats": {k: v._asdict() for k,v in psutil.net_if_stats().items()}
                    },
                "task": task_usage
                }
        except Exception as e:
            self.logger.exception(e)
            data = {'error': "{}".format(traceback.format_exception(etype=e.__class__, value=e, tb=e.__traceback__))}

        return data

    def get_task_results(self, task_id):
        if "results" in self._tasks[task_id] and self._tasks[task_id]["results"] is not None:
            return self._tasks[task_id]["results"]
        elif self._tasks[task_id]["process"].is_alive() and \
                self._tasks[task_id]["results_pipe"] is not None and \
                self._tasks[task_id]["results_pipe"].poll():
            self._tasks[task_id]["results"] = self._tasks[task_id]["results_pipe"].recv()
            return self._tasks[task_id]["results"]
        else:
            if self._tasks[task_id]["process"].is_alive():
                msg = "{} is alive, results are not yet available".format(task_id)
            elif self._tasks[task_id]["process"].exitcode is not None:
                msg = "{} exited with code {} and did not return a result".format(
                    task_id, self._tasks[task_id]["process"].exitcode)
            else:
                msg = "{} has not been started yet, results are not yet available".format(task_id)
            raise IOError(msg)

    def shutdown(self):
        for t in self._tasks:
            if self._tasks[t]["results_pipe"] is not None:
                self._tasks[t]["results_pipe"].close()
            if self._tasks[t]["process"].is_alive():
                self._tasks[t]["process"].terminate()
                self._tasks[t]["process"].join()

        self._tasks = None
        self.tasks = None


class KaleTask(multiprocessing.Process):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, results_pipe=None):
        assert results_pipe is not None, "Can not return results without a connection!"
        assert target is not None, "Can not execute, missing target value!"
        super(KaleTask, self).__init__(group, target, name, args=args, kwargs=kwargs)
        self.results = results_pipe
        self.exit = mp.Event()
        self.completed = mp.Event()
        self.logger = None

    def run(self):
        # make sure default signal handlers exist for suspend and resume
        #sigstop = signal.signal(signal.SIGSTOP, signal.SIG_DFL)
        #sigcont = signal.signal(signal.SIGCONT, signal.SIG_DFL)

        if self.logger is None:
            self.logger = logging.getLogger("KaleTask {}".format(self.pid))
            self.logger.setLevel(logging.DEBUG)
            #self.logger.addHandler(logging.handlers.RotatingFileHandler("/tmp/scratch/KaleTask_{}".format(self.pid)))
        while not self.exit.is_set():
            if self._target and not self.completed.is_set():
                self.logger.debug("KaleTask.run()\ntarget: {}\nargs: {}\nkwargs: {}\n".format(
                    self._target, self._args, self._kwargs))
                _stdout = sys.stdout
                _stderr = sys.stderr
                sys.stdout = open(str(self.pid) + ".out", "a")
                sys.stderr = open(str(self.pid) + ".err", "a")

                if len(self._kwargs) > 0:
                    out = self._target(*self._args, **self._kwargs)
                else:
                    out = self._target(*self._args)

                self.logger.debug("KaleTask.run()\nresult : {}\n".format(out))
                sys.stdout.close()
                sys.stderr.close()
                sys.stdout = _stdout
                sys.stderr = _stderr

                self.results.send(out)
                self.completed.set()
                self.results.close()
            time.sleep(1)

    def suspend(self):
        target = psutil.Process(self.pid)
        target.suspend()

    def resume(self):
        target = psutil.Process(self.pid)
        target.resume()

    def terminate(self):
        self.results.close()
        self.exit.set()

        if self.is_alive():
            target = psutil.Process(self.pid)
            # terminate any children of the target process
            kill_process_tree(self.pid)

            # terminate the target process
            super().terminate()
            if target.is_running() is None:
                target.kill()

    def join(self, timeout=None):
        self.results.close()
        self.exit.set()
        super().join(timeout)


class KaleFunctionWrapper(object):
    def __init__(self, func=None):
        assert func is not None
        setattr(self, func.__name__, func)


class KaleWorkerClient(object):
    def __init__(self, host, port, timeout=30):
        self.url = "http://{}:{}".format(host, port)
        self.logger = logging.getLogger("KaleWorkerClient {}".format(self.url))
        self._timeout = timeout

        # sanity check
        self.is_alive()

    def is_alive(self):
        retries = 3

        while retries > 0:
            try:
                self.get_service_status()
                self.logger.debug("worker {} is alive".format(self.url))
                break
            except requests.ConnectionError:
                retries -= 1
                time.sleep(0.5)

        if retries == 0:
            self.logger.debug("Unable to connect to worker {}".format(self.url))
            raise ConnectionError("Unable to connect to worker {}".format(self.url))

    def register_function_task(self, f, args=(), kwargs=None, task_name=""):
        wrapper = KaleFunctionWrapper(f)
        target = list(pickle.dumps(wrapper, protocol=pickle.HIGHEST_PROTOCOL))
        target_args = list(pickle.dumps(args, protocol=pickle.HIGHEST_PROTOCOL))
        if kwargs is not None:
            target_kwargs = list(pickle.dumps(kwargs, protocol=pickle.HIGHEST_PROTOCOL))
        else:
            target_kwargs = list(pickle.dumps({}, protocol=pickle.HIGHEST_PROTOCOL))
        response = requests.post("{}/task".format(self.url),
                                 timeout=self._timeout,
                                 data=json.dumps({
                                     "target": target,
                                     "call": f.__name__,
                                     "args": target_args,
                                     "kwargs": target_kwargs,
                                     "task_name": task_name
                                     })
                                 )
        return response.json()["id"]

    def register_method_task(self, obj, method, args=(), kwargs=None):
        self.logger.debug("register_method_task")
        assert callable(getattr(obj, method))
        target = list(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))
        target_args = list(pickle.dumps(args, protocol=pickle.HIGHEST_PROTOCOL))
        if kwargs is not None:
            target_kwargs = list(pickle.dumps(kwargs, protocol=pickle.HIGHEST_PROTOCOL))
        else:
            target_kwargs = list(pickle.dumps({}, protocol=pickle.HIGHEST_PROTOCOL))
        response = requests.post("{}/task".format(self.url),
                                 timeout=self._timeout,
                                 data=json.dumps({
                                     "target": target,
                                     "call": method,
                                     "args": target_args,
                                     "kwargs": target_kwargs,
                                     "task_name": ""
                                     })
                                 )
        return response.json()["id"]

    def get_task_output(self, task_id):
        response = requests.get("{}/task/{}/results".format(self.url, task_id), timeout=self._timeout)
        if response.ok:
            raw_output = response.json()
            try:
                output = pickle.loads(bytes(raw_output["results"]))
            except KeyError:
                raise Exception(response.json())
            return output
        else:
            raise response.raise_for_status()

    def start_task(self, task_id):
        self.logger.debug("start_task")
        response = requests.post("{}/task/{}/start".format(self.url, task_id), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            raise requests.HTTPError(response.text)

    def suspend_task(self, task_id):
        response = requests.post("{}/task/{}/suspend".format(self.url, task_id), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            raise requests.HTTPError(response.text)

    def resume_task(self, task_id):
        response = requests.post("{}/task/{}/resume".format(self.url, task_id), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            raise requests.HTTPError(response.text)

    def stop_task(self, task_id):
        response = requests.post("{}/task/{}/stop".format(self.url, task_id), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            response.raise_for_status()

    def get_tasks(self):
        response = requests.get("{}/task".format(self.url), timeout=self._timeout)
        if response.ok:
            return response.json()["tasks"]
        else:
            response.raise_for_status()

    def get_task_status(self, task_id):
        response = requests.get("{}/task/{}/status".format(self.url, task_id), timeout=self._timeout)
        if response.ok:
            return response.json()["status"]
        else:
            response.raise_for_status()

    def get_task_resources(self, task_id):
        response = requests.get("{}/task/{}/resources".format(self.url, task_id), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            response.raise_for_status()

    def get_service_status(self):
        response = requests.get("{}/".format(self.url), timeout=self._timeout)
        return response.json()["status"]

    def shutdown(self):
        response = requests.post("{}/shutdown".format(self.url), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            response.raise_for_status()
