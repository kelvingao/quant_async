import logging
from quant_async import Reports, util

class Dashboard(Reports):
    pass

if __name__ == "__main__":
    util.logToConsole("DEBUG")
    logging.getLogger("ib_async").setLevel("ERROR")
    logging.getLogger("ezib_async").setLevel("ERROR")
    
    dashboard = Dashboard(ib_port=4001, clientId=0)
    dashboard.run(port=5002)