
from quant_async import Reports

class Dashboard(Reports):
    pass

if __name__ == "__main__":
    dashboard = Dashboard(ib_host='127.0.0.1', ib_port=4001, ib_clientId=1)
    dashboard.run(port=5002)