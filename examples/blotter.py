import asyncio
import logging

from quant_async import Blotter, util

class MainBlotter(Blotter):
    pass  # we just need the name

# ===========================================
if __name__ == "__main__":
    util.logToConsole("DEBUG")
    logging.getLogger('ezib_async').setLevel(logging.ERROR)
    logging.getLogger('ib_async').setLevel(logging.ERROR)

    blotter = MainBlotter()
    asyncio.run(blotter.run())