#!/usr/bin/env python

# stdlib
import argparse
import logging
import logging.handlers
import time

# 3rd party
import requests

# local
try:
    from kale.services.worker import get_kale_id, spawn_worker, KaleWorkerClient
    from kale.services.manager import KaleManagerClient
except ImportError as e:
    raise ImportError("Unable to import kale.services!  Check your kale installation.", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start Kale Worker")
    parser.add_argument("--whost", help="DNS name or IP Address to bind a socket for this worker", default="127.0.0.1")
    parser.add_argument("--mhost", help="Kale Manager host name or IP for registration", default="127.0.0.1")
    parser.add_argument("--mport", help="Kale Manager port for registration", type=int, default=8099)
    args, unknown_args = parser.parse_known_args()

    _worker_host = "127.0.0.1"
    if args.whost:
        _host = args.whost

    _manager_host = None
    if args.mhost:
        _manager_host = args.mhost

    _manager_port = None
    if args.mport:
        _manager_port = args.mport

    rlogger = logging.getLogger()
    rlogger.setLevel(logging.DEBUG)
    
    processes = []
    clients = []
    if _manager_host and _manager_port:
        mgr = KaleManagerClient(_manager_host, _manager_port)
    elif _manager_host:
        mgr = KaleManagerClient(_manager_host)
    elif _manager_port:
        mgr = KaleManagerClient(port=_manager_port)
    else:
        mgr = KaleManagerClient()

    kale_id = get_kale_id()
    print("Spawning Kale Worker {} at {}, registering to Kale Manager at {}".format(kale_id, _worker_host, mgr.url))
    w = spawn_worker(kale_id, _worker_host, _manager_host)

    while 1:
        print("Waiting for worker connection info after registration...")
        try:
            info = mgr.get_worker(kale_id)
            print(info)
            break
        except requests.HTTPError:
            time.sleep(0.1)

    kale_worker = KaleWorkerClient(info["host"], info["port"])

    while 1:
        print("Waiting for worker to start listening...")
        try:
            kale_worker.get_service_status()
            break
        except requests.ConnectionError:
            time.sleep(0.1)
