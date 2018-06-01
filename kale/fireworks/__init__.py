# stdlib
import abc
import time
import types
from functools import wraps

# 3rd party
import requests
from fireworks.core.firework import FiretaskBase, FWAction

# locals
import kale.services.worker
import kale.services.manager


def kale_task(f):
    """Wraps a Firetask in a service.  The task will run without interfering with Fireworks itself."""

    @wraps(f)
    def spawn(self, fw_spec):
        """Performs the following steps in order.
        1. Fetch a new Kale worker id
        2. Spawn a new local Kale worker service with the given id
        3. Connect to the Kale Manager service and retrieve the worker connection information
        4. Instantiate a client to the Kale Worker service
        5. Registers the Firetask with the Kale Worker as a new task
        6. Calls the Kale Worker service to start the task
        7. Polls the Kale Worker service until the task returns
        8. Clean up of the Kale Worker service
        9. Returns the Firetask result"""
        kale_id = kale.services.worker.get_kale_id()
        p = kale.services.worker.spawn_worker(kale_id)
        manager_client = kale.services.manager.KaleManagerClient()

        # wait until worker registers before returning
        while 1:
            try:
                worker_info = manager_client.get_worker(kale_id)
                break
            except requests.exceptions.HTTPError:
                time.sleep(0.1)

        worker_client = kale.services.worker.KaleWorkerClient(worker_info["host"], worker_info["port"])

        # check that worker is alive
        while 1:
            try:
                 worker_client.get_service_status()
                 break
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)

        assert worker_info["id"] == kale_id, \
            "Kale id does not match, expected {}, received {}!".format(kale_id, worker_info["id"])

        # register this method as a new task to run
        task_id = worker_client.register_method_task(self, f.__name__, (fw_spec,))

        # start the task
        try:
            out = worker_client.start_task(task_id)
            assert "pid" in out
        except requests.exceptions.HTTPError as e:
            print(e)
        except AssertionError:
            print(out)

        # poll for the return value
        while 1:
            try:
                value = worker_client.get_task_output(task_id)
                break
            except requests.exceptions.HTTPError:
                time.sleep(1)

        # clean up worker process
        worker_client.stop_task(task_id)
        worker_client.shutdown()
        try:
            p.join(timeout=5)
        except TimeoutError:
            print("Service did not shutdown within 5 seconds, terminating")
            p.terminate()
            p.join()

        # return
        if isinstance(value, FWAction):
            return value
        elif value is None:
            return FWAction()
        else:
            raise ValueError("Expected FWAction, instead received {}".format(type(value)))
    return spawn


def override_new(cls, *args, **kwargs):
    o = FiretaskBase.__original_new__(cls, *args, **kwargs)
    setattr(o, "original_run_task", o.run_task)
    setattr(o, "run_task", types.MethodType(kale_task(o.original_run_task), o))
    return o


FiretaskBase.__original_new__ = FiretaskBase.__new__
FiretaskBase.__new__ = override_new
