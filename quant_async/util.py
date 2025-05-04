import logging
import sys

def logToConsole(level=logging.INFO):
    """Create a log handler that logs to the console."""
    logger = logging.getLogger()
    stdHandlers = [
        h
        for h in logger.handlers
        if type(h) is logging.StreamHandler and h.stream is sys.stderr
    ]

    if stdHandlers:
        # if a standard stream handler already exists, use it and
        # set the log level for the ib_async namespace only
        logging.getLogger("quant_async").setLevel(level)
    else:
        # else create a new handler
        logger.setLevel(level)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)