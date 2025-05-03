
from quant_async import Reports

class Dashboard(Reports):
    pass

if __name__ == "__main__":
    dashboard = Dashboard(ib_port=4001, clientId=0)
    dashboard.run(port=5002)