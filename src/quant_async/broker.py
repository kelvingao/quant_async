import logging
import asyncio
import pandas as pd

from abc import ABCMeta
from ib_async import Contract, Position
from ezib_async import ezIBAsync

from quant_async import tools
from quant_async.blotter import (
    Blotter, load_blotter_args
)

from quant_async.instrument import Instrument

class Broker(metaclass=ABCMeta):
    """Broker class initilizer (abstracted, parent class of ``Algo``)

    :Parameters:

        instruments : list
            List of IB contract tuples
        ibclient : int
            IB TWS/GW Port to use (default: 4001)
        ibport : int
            IB TWS/GW Client ID (default: 998)
        ibserver : string
            IB TWS/GW Server hostname (default: localhost)
    """

    def __init__(self, instruments, ibclient=998, ibport=4001, ibhost="localhost"):

        # detect running strategy
        self.strategy = str(self.__class__).split('.')[-1].split("'")[0]

        # initilize class logger
        self._logger = logging.getLogger(__name__)
        
        # -----------------------------------
        # assign default vals if not propogated from algo
        if not hasattr(self, 'timezone'):
            self.timezone = "UTC"
        if not hasattr(self, 'tick_window'):
            self.tick_window = 1000
        if not hasattr(self, 'bar_window'):
            self.bar_window = 100
        if not hasattr(self, 'last_price'):
            self.last_price = {}
        if not hasattr(self, 'backtest'):
            self.backtest = False
        if not hasattr(self, 'sms_numbers'):
            self.sms_numbers = []
        if not hasattr(self, 'trade_log_dir'):
            self.trade_log_dir = None
        if not hasattr(self, 'blotter_name'):
            self.blotter_name = None
        
        # -----------------------------------
        # connect to IB
        self.ibclient = int(ibclient)
        self.ibport = int(ibport)
        self.ibhost = str(ibhost)

        self.ezib = ezIBAsync()
        
        self.instruments = {}
        self.instrument_combos = {}
        self.symbols = []
        self._instruments = instruments
        
        # -----------------------------------
        # load blotter settings

        self.blotter_args = load_blotter_args(
            self.blotter_name, logger=self._logger)
        self.blotter = Blotter(**self.blotter_args)
        
        
        # connect to IB and create contracts
        # self.initialized = False
        # asyncio.create_task(self._initialize())

    # -------------------------------------------
    async def initialize(self):
        await self._connectAsync()
        await self._createContractsAsync()
        # self.initialized = True
    
    # -------------------------------------------
    async def _connectAsync(self):
        connection_tries = 0
        while not self.ezib.connected:
            await self.ezib.connectAsync(ibclient=self.ibclient,
                                ibport=self.ibport, ibhost=self.ibhost)
            await asyncio.sleep(1)
            if not self.ezib.connected:
                # print('*', end="", flush=True)
                connection_tries += 1
                if connection_tries > 10:
                    self._logger.info(
                        "Cannot connect to Interactive Brokers...")
                    sys.exit(0)

        self._logger.info("Connection established...")

    # -------------------------------------------
    async def _createContractsAsync(self):
        
        instrument_tuples_dict = {}
        for instrument in self._instruments:
            try:
                if isinstance(instrument, Contract):
                    instrument = self.ezib.contract_to_tuple(instrument)
                else:
                    instrument = tools.create_ib_tuple(instrument)
                contract = await self.ezib.createContract(*instrument)
                contractString = self.ezib.contractString(contract)
                instrument_tuples_dict[contractString] = self.ezib.contract_to_tuple(contract)
                
            except Exception as e:
                self._logger.error(f"creating contract {instrument}: {e}")

        self.instruments = instrument_tuples_dict
        self.symbols = list(self.instruments.keys())
        
        # add instruments to blotter in case they do not exist
        self.blotter.register(self.instruments)

        # # create contracts
        # instrument_tuples_dict = {}
        # for instrument in instruments:
        #     try:
        #         contractString = self.ezib.contractString(instrument)
        #         instrument_tuples_dict[contractString] = instrument
        #         # await self.ezib.createContract(instrument)
        #     except Exception as e:
        #         self._logger.error(f"ERROR when registering instruments and sybmols: {e}")

        # self.instruments = instrument_tuples_dict
        # self.symbols = list(self.instruments.keys())
        # self.instrument_combos = {}
    
    # -------------------------------------------
    def _disconnect(self):
        """
        Disconnects from the Interactive Brokers API (TWS/Gateway) and cleans up resources.

        """
        try:
            # disconnect
            if self.ezib.connected:
                self._logger.info("Disconnecting from IB")
                self.ezib.disconnect()
                self._logger.info("Disconnected.")
        except Exception as e:
            self._logger.error(f"Error during disconnection: {str(e)}")
    
    # ---------------------------------------
    def get_position(self, symbol):
        symbol = self.get_symbol(symbol)

        if self.backtest:
            position = 0
            avgCost = 0.0

            if self.datastore.recorded is not None:
                data = self.datastore.recorded
                col = symbol.upper() + '_POSITION'
                position = data[col].values[-1]
                if position != 0:
                    pos = data[col].diff()
                    avgCost = data[data.index.isin(pos[pos != 0][-1:].index)
                                   ][symbol.upper() + '_OPEN'].values[-1]
            return {
                    "symbol": symbol,
                    "position": position,
                    "avgCost":  avgCost,
                    "account":  "Backtest"
                }

        elif symbol in self.ezib.position:
            return self.ezib.position[symbol]
        
        return Position()

        # return {
        #     "symbol": symbol,
        #     "position": 0,
        #     "avgCost":  0.0,
        #     "account":  None
        # }

    # ---------------------------------------
    # UTILITY FUNCTIONS
    # ---------------------------------------
    def get_instrument(self, symbol):
        """
        A string subclass that provides easy access to misc
        symbol-related methods and information using shorthand.
        Refer to the `Instruments API <#instrument-api>`_
        for available methods and properties

        Call from within your strategy:
        ``instrument = self.get_instrument("SYMBOL")``

        :Parameters:
            symbol : string
                instrument symbol

        """
        instrument = Instrument(self.get_symbol(symbol))
        instrument._set_parent(self)
        instrument._set_windows(ticks=self.tick_window, bars=self.bar_window)

        return instrument
    
    # ---------------------------------------
    @staticmethod
    def get_symbol(symbol):
        if not isinstance(symbol, str):
            if isinstance(symbol, dict):
                symbol = symbol['symbol']
            elif isinstance(symbol, pd.DataFrame):
                symbol = symbol[:1]['symbol'].values[0]

        return symbol