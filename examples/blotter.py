import asyncio
from quant_async import Blotter

class MainBlotter(Blotter):
    pass  # we just need the name

# ===========================================
if __name__ == "__main__":
    blotter = MainBlotter()
    asyncio.run(blotter.run())