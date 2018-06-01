# stdlib
import sys
import random

# 3rd party
import sanic.exceptions

# locals
import kale.services.worker

if __name__ == "__main__":
    assert len(sys.argv) == 2, "kale_id is required"
    kale_id = sys.argv[1]
    app = kale.services.worker.KaleWorker(kale_id)

    # Choose a random port and try to instantiate, on failure select a new random port
    port = random.randint(32768, 61000)
    try:
        app.run(host="0.0.0.0", port=port)
    except sanic.exceptions.ServerError as e:
        print(e)
        raise
