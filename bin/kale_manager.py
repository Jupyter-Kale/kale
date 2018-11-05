#!/usr/bin/env python

import time
import argparse

try:
    import kale.services.manager
except ImportError as e:
    raise ImportError("An installation of kale was not found!  Import of kale.services.manager failed.", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="DNS name or IP Address to bind a socket, default = 127.0.0.1")
    parser.add_argument("--port", help="Port to listen on, default = 8099", type=int)

    args = parser.parse_args()
    print(args.echo)

    _host = "127.0.0.1"
    if args.host:
        _host = args.host

    _port = 8099
    if args.port:
        _port = args.port

    print("Spawning Kale Manager at {}:{}".format(_host, _port))
    mgr_proc = kale.services.manager.spawn_manager(host=_host, port=_port)
    manager = kale.services.manager.KaleManagerClient()

    assert mgr_proc.is_alive()

    # wait until manager is alive
    while 1:
        try:
            manager.list_workers()
            break
        except Exception as e:
            time.sleep(1)

    print("Kale Manager is alive at {}".format(manager.url))
