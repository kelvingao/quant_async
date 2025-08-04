import asyncio
import logging
import argparse
import pandas as pd

from abc import ABCMeta, abstractmethod
from quant_async.broker import Broker

class Algo(Broker, metaclass=ABCMeta):

    def __init__(self, instruments, resolution="1T",
                 tick_window=1, bar_window=100, timezone="UTC", preload=None,
                 continuous=True, blotter=None, sms=None, log=None,
                 backtest=False, start=None, end=None, data=None, output=None,
                 ibclient=998, ibport=4001, ibhost="localhost", **kwargs):
    
        # detect algo name
        self.name = str(self.__class__).split('.')[-1].split("'")[0]

        # initilize algo logger
        self._logger = logging.getLogger("quant_async.algo")
        
        # override args with any (non-default) command-line args
        self.args = {arg: val for arg, val in locals().items(
        ) if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())
    
        # -----------------------------------
        # assign algo params
        self.bars = pd.DataFrame()
        self.ticks = pd.DataFrame()
        self.quotes = {}
        self.books = {}
        
        # -----------------------------------
        # initiate broker/order manager
        super().__init__(instruments, **{
            arg: val for arg, val in self.args.items() if arg in (
                'ibport', 'ibclient', 'ibhost')})
    
    # ---------------------------------------
    def load_cli_args(self):
        """
        Parse command line arguments and return only the non-default ones

        :Retruns: dict
            a dict of any non-default args passed on the command-line.
        """
        parser = argparse.ArgumentParser(
            description='QTPyLib Algo',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument('--ibport', default=self.args["ibport"],
                            help='IB TWS/GW Port', type=int)
        parser.add_argument('--ibclient', default=self.args["ibclient"],
                            help='IB TWS/GW Client ID', type=int)
        parser.add_argument('--ibhost', default=self.args["ibhost"],
                            help='IB TWS/GW Server hostname')
        parser.add_argument('--sms', default=self.args["sms"],
                            help='Numbers to text orders', nargs='+')
        parser.add_argument('--log', default=self.args["log"],
                            help='Path to store trade data')
        parser.add_argument('--backtest', default=self.args["backtest"],
                            help='Work in Backtest mode (flag)',
                            action='store_true')
        parser.add_argument('--start', default=self.args["start"],
                            help='Backtest start date')
        parser.add_argument('--end', default=self.args["end"],
                            help='Backtest end date')
        parser.add_argument('--data', default=self.args["data"],
                            help='Path to backtester CSV files')
        parser.add_argument('--output', default=self.args["output"],
                            help='Path to save the recorded data')
        parser.add_argument('--blotter',
                            help='Log trades to this Blotter\'s MySQL')
        parser.add_argument('--continuous', default=self.args["continuous"],
                            help='Use continuous Futures contracts (flag)',
                            action='store_true')

        # only return non-default cmd line args
        # (meaning only those actually given)
        cmd_args, _ = parser.parse_known_args()
        args = {arg: val for arg, val in vars(
            cmd_args).items() if val != parser.get_default(arg)}
        return args

    # ---------------------------------------
    async def run(self):
        """Starts the algo

        Connects to the Blotter, processes market data and passes
        tick data to the ``on_tick`` function and bar data to the
        ``on_bar`` methods.
        """
        try:
            
            await self.initialize()

            
            # initiate strategy
            self.on_start()

            # listen for RT data
            await self.blotter.stream(
                symbols=self.symbols,
                tz=self.timezone,
                quote_handler=self._quote_handler,
                book_handler=self._book_handler
            )
            
            # Create an event that will never be set
            # stop_event = asyncio.Event()
            
            # Setup signal handlers for graceful shutdown
            # loop = asyncio.get_running_loop()
            # for sig in (signal.SIGINT, signal.SIGTERM):
            #     loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))
            
            # Wait for the event (which will never be set unless we call _shutdown)
            # await stop_event.wait()

        except asyncio.CancelledError:
            pass
            # This is expected when Ctrl+C is pressed
        except (KeyboardInterrupt, SystemExit):
            print('Ctrl-C Gotted')
        except Exception as e:
            self._logger.error(f"Error: {e}")
        finally:
            # Cleanup
            await self._cleanup()
            
            # # This is expected when Ctrl+C is pressed
            # print(
            #     "\n\n>>> Interrupted with Ctrl-c...\n(waiting for running tasks to be completed)\n")
            # # Cancel all running tasks except the current one
            # for task in asyncio.all_tasks():
            #     if task is not asyncio.current_task():
            #         task.cancel()
            # sys.exit(1)

        # except Exception as e:
        #     self._logger.error(f"Error: {e}")
        # finally:
        #     # Cleanup
        #     await self._cleanup()

    # ---------------------------------------
    async def _shutdown(self):
        """Handle graceful shutdown."""
        print(
                "\n\n>>> Interrupted with Ctrl-c...\n(waiting for running tasks to be completed)\n")
        # Cancel all running tasks except the current one
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
    # ---------------------------------------
    async def _cleanup(self):
        """Handle graceful shutdown."""
        self._disconnect()
        
    
    # ---------------------------------------
    @abstractmethod
    def on_start(self):
        """
        Invoked once when algo starts. Used for when the strategy
        needs to initialize parameters upon starting.

        """
        # raise NotImplementedError("Should implement on_start()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_quote(self, instrument):
        """
        Invoked on every quote captured for the selected instrument.
        This is where you'll write your strategy logic for quote events.

        :Parameters:

            symbol : string
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_quote()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_tick(self, instrument):
        """
        Invoked on every tick captured for the selected instrument.
        This is where you'll write your strategy logic for tick events.

        :Parameters:

            symbol : string
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_tick()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_bar(self, instrument):
        """
        Invoked on every tick captured for the selected instrument.
        This is where you'll write your strategy logic for tick events.

        :Parameters:

            instrument : object
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_bar()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_orderbook(self, instrument):
        """
        Invoked on every change to the orderbook for the selected instrument.
        This is where you'll write your strategy logic for orderbook events.

        :Parameters:

            symbol : string
                `Instruments Object <#instrument-api>`_

        """
        # raise NotImplementedError("Should implement on_orderbook()")
        pass

    # ---------------------------------------
    @abstractmethod
    def on_fill(self, instrument, order):
        """
        Invoked on every order fill for the selected instrument.
        This is where you'll write your strategy logic for fill events.

        :Parameters:

            instrument : object
                `Instruments Object <#instrument-api>`_
            order : object
                Filled order data

        """
        # raise NotImplementedError("Should implement on_fill()")
        pass
    
    # ---------------------------------------
    async def _book_handler1(self, book):
        # symbol = self.ezib.contractString(book.contract)
        # del book['symbol']
        # del book['kind']

        # self.books[symbol] = book
        print(type(book))
        self.on_orderbook(self.get_instrument('EURUSD'))
    
    # ---------------------------------------
    async def _book_handler(self, book):
        symbol = book['symbol']
        del book['symbol']
        del book['kind']

        self.books[symbol] = book
        self.on_orderbook(self.get_instrument(symbol))
    
    # ---------------------------------------
    async def _quote_handler(self, quote):
        del quote['kind']
        self.quotes[quote['symbol']] = quote
        self.on_quote(self.get_instrument(quote))