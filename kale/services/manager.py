# stdlib
import multiprocessing
import logging
import time

# 3rd party
import requests
import sanic
import sanic.exceptions
import sanic.response

# local
from . import db

app = sanic.Sanic()
ws = db.WorkerStore()


def spawn_manager(host="0.0.0.0", port=8099):
    logger = logging.getLogger(__name__)
    logger.debug("spawn manager")
    p = multiprocessing.Process(target=app.run, args=[host, port])
    p.start()
    return p


@app.route("/worker", methods=["POST"])
def add_worker(request):
    ws.add(request.json["id"], request.json["protocol"], request.json["host"], request.json["port"])
    return sanic.response.json({"status": "worker added"})


@app.route("/worker/<wid>", methods=["GET"])
def find_worker(request, wid):
    worker = ws.find(wid)
    if worker is None:
        return sanic.response.json(body=[{"error": "{} not found".format(wid)}], status=404)
    data = {
        "id": worker[0],
        "protocol": worker[1],
        "host": worker[2],
        "port": worker[3]
        }
    return sanic.response.json(data)


@app.route("/worker/<wid>", methods=["DELETE"])
def remove_worker(request, wid):
    worker = ws.find(wid)
    if worker is None:
        return sanic.response.json(body=[{"error": "{} not found".format(wid)}], status=404)
    try:
        ws.remove(wid)
    except Exception as e:
        return sanic.response.json(body=[{"error": "{}".format(e.args)}], status=404)

    return sanic.response.json(body={"status": "worker removed"})


@app.route("/worker", methods=["GET"])
def list_workers(request):
    try:
        workers = ws.list()
        data = []
        for w in workers:
            data.append({
                "id": w[0],
                "protocol": w[1],
                "host": w[2],
                "port": w[3]
                })
        return sanic.response.json(data)
    except Exception as e:
        return sanic.response.json({"error": e.args})


@app.route("/shutdown", methods=["POST"])
def shutdown(request):
    try:
        delay = 3
        app.loop.call_later(delay, app.stop)
        return sanic.response.json({"status": "Shutting down in {} seconds!".format(delay)})
    except Exception as e:
        return sanic.response.json({"error": e.args})


@app.route("/status", methods=["GET"])
def get_status(request):
    try:
        status = {
            "num_workers": len(ws.list())
        }
        return sanic.response.json({"status": status})
    except Exception as e:
        return sanic.response.json({"error": e.args})


class KaleManagerClient(object):
    def __init__(self, host="127.0.0.1", port=8099, timeout=3):
        self.url = "http://{}:{}".format(host, port)
        self.logger = logging.getLogger("KaleManagerClient")
        self._timeout = timeout

        while 1:
            try:
                self.list_workers()
                break
            except requests.ConnectionError:
                time.sleep(0.5)
            except Exception as e:
                self.logger.exception(e)
                raise

        self.logger.debug("manager is alive")

    def add_worker(self, kale_id, protocol, host, port):
        self.logger.debug("add_worker")
        response = requests.post("{}/worker".format(self.url),
            timeout=self._timeout,
            body={
            "id": kale_id,
            "protocol": protocol,
            "host": host,
            "port": port
            })
        if response.ok:
            return response.json()
        else:
            response.raise_for_status()

    def remove_worker(self, kale_id):
        self.logger.debug("remove_worker")
        response = requests.delete("{}/worker/{}".format(self.url, kale_id), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            response.raise_for_status()

    def get_worker(self, kale_id):
        self.logger.debug("get_worker")
        response = requests.get("{}/worker/{}".format(self.url, kale_id), timeout=self._timeout)
        if response.ok:
            return response.json()
        else:
            response.raise_for_status()

    def list_workers(self):
        self.logger.debug("list_workers")
        response = requests.get("{}/worker".format(self.url), timeout=self._timeout)
        if response.ok:
            self.logger.debug(response.json())
            return response.json()
        else:
            self.logger.debug("response {} {}".format(response.reason, response.text))
            response.raise_for_status()

    def shutdown(self):
        self.logger.debug("shutdown")
        response = requests.post("{}/shutdown".format(self.url), timeout=self._timeout)
        if response.ok:
            msg = response.json()
            if "status" in msg:
                return msg["status"]
            elif "error" in msg:
                self.logger.debug(msg["error"])
        else:
            response.raise_for_status()

    def get_status(self):
        self.logger.debug("status")
        response = requests.get("{}/status".format(self.url), timeout=self._timeout)
        if response.ok:
            msg = response.json()
            if "status" in msg:
                return msg["status"]
            elif "error" in msg:
                self.logger.debug(msg["error"])
        else:
            response.raise_for_status()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8099)
