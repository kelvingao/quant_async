"""
MIT License

Copyright (c) 2025 Kelvin Gao

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
import sys
import signal
import asyncio
import logging
import tempfile
import pickle
import glob
import argparse
import pandas as pd
import zmq
import aiozmq
import asyncpg
import msgspec
import pytz

from datetime import datetime, timezone
from typing import Any
from numpy import isnan
from ezib_async.ezib import ezIBAsync
from quant_async.monitoring import SystemMonitor

from .objects import OrderBookSnapshot, DOMLevel
from . import tools

path = {
    "library": os.path.dirname(os.path.realpath(__file__)),
    "caller": os.path.dirname(os.path.realpath(sys.argv[0]))
}


# cash ticks
cash_ticks: dict[str, Any] = {}


class Blotter:
    """
    Blotter class for handling Interactive Brokers connections and market data.
    
    This class manages the connection to Interactive Brokers, processes and logs market data.
    
    Args:
        ibhost (str, optional): IB TWS/GW Server hostname. Defaults to "localhost".
        ibport (int, optional): TWS/GW Port to use. Defaults to 4001.
        ibclient (int, optional): TWS/GW Client ID. Defaults to 999.
        name (str, optional): Name of the blotter instance. Defaults to class name.
        symbols (str, optional): Path to CSV file with symbols to monitor. Defaults to "symbols.csv".
        orderbook (bool, optional): Whether to request market depth data. Defaults to False.
        **kwargs: Additional keyword arguments.
    """

    def __init__(self, ibhost="localhost", ibport=4001, ibclient=996, name=None, 
                 symbols="symbols.csv", orderbook=False, zmqport="12345", zmqtopic=None, 
                 dbskip=False, dbhost="localhost", dbport=5432, dbuser="quant_async", 
                 dbpass="", dbname="quant_async", **kwargs):
        # Initialize class logger
        self._logger = logging.getLogger("quant_async.blotter")
        
        # Set blotter name
        self.name = str(self.__class__).split('.')[-1].split("'")[0].lower()
        if name is not None:
            self.name = name
            
        # -------------------------------
        # work default values
        # -------------------------------
        if zmqtopic is None:
            zmqtopic = "_quant_async_" + str(self.name.lower()) + "_"
            
        self.pool = None
        self.socket = None
        self.stop_event = None
        
        # Initialize monitoring system
        self.monitor = SystemMonitor()
        
        # Initialize symbol cache with monitoring
        self.symbol_ids = {}
        
        # Initialize DataStore for enhanced data operations
        self.datastore = None
        
        # Store tickers and bars for market data
        self.tickers = {}  # symbol -> ticker object
        self.bars = {}     # symbol -> bars object
        
        # Store arguments
        self.args = {arg: val for arg, val in locals().items()
                    if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())

        # Symbols file and monitoring settings
        if os.path.isabs(self.args['symbols']):
            self.symbols = self.args['symbols']
        else:
            # Fix: Avoid duplicate path concatenation when running from examples/
            symbols_path = self.args['symbols']
            if os.path.exists(symbols_path):
                # If the relative path exists from current directory, use it directly
                self.symbols = os.path.abspath(symbols_path)
            else:
                # Otherwise, concatenate with caller path
                self.symbols = os.path.join(path['caller'], symbols_path)
        
        # Initialize args cache file path
        self.args_cache_file = os.path.join(tempfile.gettempdir(), f"{self.name}.quant_async")
        
        # Flag to track if this is a duplicate run
        self.duplicate_run = False
        
        # Log initialization with configuration summary
        self._logger.info(f"Initializing Blotter '{self.name}' with enhanced monitoring")
        self._logger.info(f"Configuration - IB: {self.args['ibhost']}:{self.args['ibport']}[{self.args['ibclient']}], "
                         f"DB: {'enabled' if not self.args['dbskip'] else 'disabled'}, "
                         f"ZMQ: {self.args['zmqport']}, OrderBook: {self.args['orderbook']}")
    
    # ---------------------------------------
    def _register_events_handler(self):
        """Register event handlers for market data from IB."""
        self._logger.info("🔗 Registering IB event handlers...")
        
        self.ezib.updateMarketDepthEvent += self.on_orderbook_received
        self._logger.debug("✅ Registered updateMarketDepthEvent handler")
        
        # Add handler for ticker updates
        self.ezib.quoteUpdateEvent += self.on_quote_received
        self._logger.debug("✅ Registered quoteUpdateEvent handler")
        
        self._logger.info("🎯 Event handlers registration complete")
    
    # ---------------------------------------
    @staticmethod
    async def _blotter_file_running():
        """
        Check if the blotter process is running.
        
        Returns:
            bool: True if the process is running, False otherwise.
        """
        try:
            # Create a subprocess to run pgrep
            command = f'pgrep -f {sys.argv[0]}'
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Wait for the process to complete and get output
            stdout, _ = await process.communicate()
            
            # Process the output
            stdout_list = stdout.decode('utf-8').strip().split('\n')
            stdout_list = list(filter(None, stdout_list))
            
            return len(stdout_list) > 0
        except Exception as e:
            logging.getLogger('quant_async.blotter').error(f"Error checking if blotter is running: {e}")
            return False
    
    # ---------------------------------------
    async def _check_unique_blotter(self):
        """
        Check if another instance of this blotter is already running.
        If another instance is running, exit with an error.
        """
        try:
            # Check if args cache file exists
            if os.path.exists(self.args_cache_file):
                # Temp file found - check if process is really running
                # or if this file wasn't deleted due to crash
                if not await self._blotter_file_running():
                    # Process not running, remove old cache file
                    await self._remove_cached_args()
                else:
                    # Process is running, this is a duplicate
                    self.duplicate_run = True
                    self._logger.error(f"Blotter '{self.name}' is already running...")
                    sys.exit(1)
            
            # Write current args to cache file
            await self._write_cached_args()
            self._logger.info(f"Started Blotter instance: {self.name}")
            
        except Exception as e:
            self._logger.error(f"Error checking unique blotter: {e}")
            sys.exit(1)
    
    # ---------------------------------------
    async def _remove_cached_args(self):
        """Remove cached arguments file."""
        if os.path.exists(self.args_cache_file):
            try:
                os.remove(self.args_cache_file)
                self._logger.debug(f"Removed cached args file: {self.args_cache_file}")
            except Exception as e:
                self._logger.error(f"Error removing cached args file: {e}")
    
    # ---------------------------------------
    async def _read_cached_args(self):
        """
        Read cached arguments from file.
        
        Returns:
            dict: Cached arguments or empty dict if file doesn't exist.
        """
        if os.path.exists(self.args_cache_file):
            try:
                with open(self.args_cache_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                self._logger.error(f"Error reading cached args: {e}")
        return {}
    
    # ---------------------------------------
    async def _write_cached_args(self):
        """Write arguments to cache file."""
        try:
            with open(self.args_cache_file, 'wb') as f:
                pickle.dump(self.args, f)
            
            # Set file permissions
            os.chmod(self.args_cache_file, 0o666)
            self._logger.debug(f"Wrote cached args to: {self.args_cache_file}")
        except Exception as e:
            self._logger.error(f"Error writing cached args: {e}")
    
    # ---------------------------------------
    def load_cli_args(self):
        """
        Parse command line arguments and return only the non-default ones.
        
        Returns:
            dict: A dict of any non-default args passed on the command-line.
        """
        parser = argparse.ArgumentParser(
            description='Quant Async Blotter',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
            
        parser.add_argument('--ibhost', default=self.args['ibhost'],
                          help='IB TWS/GW Server hostname', required=False)
        parser.add_argument('--ibport', default=self.args['ibport'],
                          help='TWS/GW Port to use', required=False)
        parser.add_argument('--ibclient', default=self.args['ibclient'],
                          help='TWS/GW Client ID', required=False)
        parser.add_argument('--symbols', default=self.args['symbols'],
                          help='Path to CSV file with symbols to monitor', required=False)
        parser.add_argument('--orderbook', action='store_true', default=self.args['orderbook'],
                          help='Request market depth data', required=False)
        
        # Database connection parameters
        parser.add_argument('--dbhost', default='localhost', 
                          help='Database hostname', required=False)
        parser.add_argument('--dbport', default=5432, type=int,
                          help='Database port', required=False)
        parser.add_argument('--dbuser', default='quant_async', 
                          help='Database username', required=False)
        parser.add_argument('--dbpass', default='', 
                          help='Database password', required=False)
        parser.add_argument('--dbname', default='quant_async', 
                          help='Database name', required=False)
        parser.add_argument('--dbskip', action='store_true', 
                          help='Skip database connection', required=False)
        
        # Only return non-default cmd line args
        # (meaning only those actually given)
        cmd_args, _ = parser.parse_known_args()
        args = {k: v for k, v in vars(cmd_args).items() if v != parser.get_default(k)}
        return args
    
    # ---------------------------------------
    async def _watch_symbols_file(self):
        """
        Watch the symbols file for changes and update subscriptions accordingly.
        Based on the original synchronous implementation but converted to async.
        """
        
        # Initialize tracking variables
        first_run = True
        prev_contracts = []
        db_modified = 0
        
        while True:
            try:
                # no symbols file
                if not os.path.exists(self.symbols):
                    self._logger.info(f"Creating symbols file: {self.symbols}")
                    # create empty symbols file
                    df = pd.DataFrame(columns=['symbol', 'sec_type', 'exchange', 
                                             'currency', 'expiry', 'strike', 'opt_type'])
                    df.to_csv(self.symbols, index=False)
                    os.chmod(self.symbols, 0o666)  # Make writable by all
                else:
                    # Small delay before checking file stats
                    await asyncio.sleep(0.1)
                    
                    # Read file stats
                    db_data = os.stat(self.symbols)
                    db_size = db_data.st_size
                    db_last_modified = db_data.st_mtime
                    
                    # empty file
                    if db_size == 0:
                        if prev_contracts:
                            self._logger.info('Cancel market data...')
                            for contract in prev_contracts:
                                contract_obj = await self.ezib.createContract(*contract)
                                self.ezib.cancelMarketData(contract_obj)
                                if self.orderbook:
                                    self.ezib.cancelMarketDepth(contract_obj)
                            prev_contracts = []
                        # await asyncio.sleep(1)
                        continue
                    
                    # file not modified
                    if not first_run and db_last_modified == db_modified:
                        # await asyncio.sleep(0.1)
                        continue
                    
                    # Update modification time
                    db_modified = db_last_modified
                    
                    # Read symbols file
                    df = pd.read_csv(self.symbols)
                    if df.empty:
                        # await asyncio.sleep(0.1)
                        continue
                    
                    # Filter based on expiry dates
                    current_date = datetime.now()
                    df = df[(
                        (df['expiry'] < 1000000) & 
                        (df['expiry'] >= int(current_date.strftime('%Y%m'))) # current month
                    ) | (
                        (df['expiry'] >= 1000000) & 
                        (df['expiry'] >= int(current_date.strftime('%Y%m%d'))) # current day
                    ) | isnan(df['expiry'])]
                    
                    # 格式化数据
                    df['expiry'] = df['expiry'].fillna(0).astype(int).astype(str)
                    df.loc[df['expiry'] == "0", 'expiry'] = ""
                    df = df[df['sec_type'] != 'BAG']
                    df['opt_type'] = df['opt_type'].fillna("").astype(str)
                    df['exchange'] = df['exchange'].fillna("").astype(str)
                    df['currency'] = df['currency'].fillna("").astype(str)
                    
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df.fillna({col: ''}, inplace=True)
                        else:
                            df.fillna({col: 0}, inplace=True)
                    
                    # Save cleaned data back to file
                    df.to_csv(self.symbols, index=False)
                    os.chmod(self.symbols, 0o666)  # Make writable by all
                    
                    # Filter out commented symbols
                    df = df[~df['symbol'].astype(str).str.startswith('#')]
                    
                    # Convert to list of tuples
                    contracts = [tuple(x) for x in df.values]
                    
                    if first_run:
                        first_run = False
                    else:
                        # symbols file changed
                        for contract in prev_contracts:
                            if contract not in contracts:

                                contract_obj = await self.ezib.createContract(*contract)
                                if contract_obj:
                                    self.ezib.cancelMarketData(contract_obj)
                                    if self.args['orderbook']:
                                        self.ezib.cancelMarketDepth(contract_obj)
                                    contract_string = self.ezib.contractString(contract).split('_')[0]
                                    self._logger.info('Contract Removed [%s]', contract_string)
                                
                    # new contract
                    for contract in contracts:
                        if contract not in prev_contracts:
                            self._logger.info(f'Processing new contract: {contract}')
                            
                            contract_obj = await self.ezib.createContract(*contract)
                            if contract_obj:
                                self._logger.info(f'Contract created successfully: {contract_obj}')
                                
                                try:
                                    await self.ezib.requestMarketData(contract_obj)
                                    self._logger.info(f'Market data requested for contract: {contract_obj}')
                                except Exception as e:
                                    self._logger.error(f'Failed to request market data: {e}')
                                
                                if self.args['orderbook']:
                                    try:
                                        self.ezib.requestMarketDepth(contract_obj)
                                        self._logger.info(f'Market depth requested for contract: {contract_obj}')
                                    except Exception as e:
                                        self._logger.error(f'Failed to request market depth: {e}')

                                contract_string = self.ezib.contractString(contract).split('_')[0]
                                self._logger.info('Contract Added [%s]', contract_string)
                            else:
                                self._logger.error(f'Failed to create contract for: {contract}')
                    
                    # Update previous contracts list
                    prev_contracts = contracts
                
                # Wait before next check
                await asyncio.sleep(1)
                
            except Exception as e:
                self._logger.error(f"Error watching symbols file: {e}")
                await asyncio.sleep(1)
    
    # ---------------------------------------
    async def broadcast(self, ticker_snap, kind):
        """
        Broadcast market data using ZeroMQ.
        
        This method serializes the data and sends it over ZeroMQ with the appropriate topic.
        
        Args:
            ticker_snap (dict): The data to broadcast
            kind (str): The kind of data (e.g., "TICK", "BAR", "QUOTE", "ORDERBOOK")
        """
        # DEBUG: Log broadcast attempt
        self._logger.debug(f"🚀 broadcast() called with kind={kind}")
        
        if self.socket is None:
            self._logger.error("❌ There is no socket connection setup")
            self.monitor.record_error("zmq_socket_missing", "No ZeroMQ socket available")
            return
        
        try:
            # Record message for monitoring
            symbol = ticker_snap.get('symbol', 'unknown') if isinstance(ticker_snap, dict) else 'unknown'
            self.monitor.record_message(kind, symbol)
            
            # DEBUG: Log before serialization
            self._logger.debug(f"📦 Serializing {kind} data for {symbol}")
            
            # Serialize and broadcast
            with self.monitor.time_operation(f"broadcast_{kind.lower()}"):
                serialized = msgspec.msgpack.encode(ticker_snap)
                self.socket.write([serialized])
                
            self._logger.info(f"✅ Successfully broadcasted {kind} for {symbol}")
            self._logger.debug(f"📊 Data size: {len(serialized)} bytes")
            
        except (aiozmq.ZmqStreamClosed, ConnectionError) as e:
            error_msg = f"Error broadcasting {kind} data via ZeroMQ: {e}"
            self._logger.error(error_msg)
            self.monitor.record_error("zmq_broadcast_error", error_msg)
        except Exception as e:
            error_msg = f"Unexpected error in broadcast: {e}"
            self._logger.error(error_msg)
            self.monitor.record_error("broadcast_error", error_msg)

    # ---------------------------------------
    async def on_orderbook_received(self, tickers):
        for t in tickers:
            # Extract bids and asks from the ticker
            bids = []
            asks = []
            
            # Process DOM bids if available
            if hasattr(t, 'domBids') and t.domBids:
                for bid in t.domBids:
                    bids.append(DOMLevel(
                        price=bid.price,
                        size=bid.size,
                        market_maker=bid.marketMaker if hasattr(bid, 'marketMaker') else ""
                    ))
            
            # Process DOM asks if available
            if hasattr(t, 'domAsks') and t.domAsks:
                for ask in t.domAsks:
                    asks.append(DOMLevel(
                        price=ask.price,
                        size=ask.size,
                        market_maker=ask.marketMaker if hasattr(ask, 'marketMaker') else ""
                    ))

            # contractString = self.ezib.tickerSymbol(tickerId)
            # Create the OrderBookSnapshot
            try:
                orderbook = OrderBookSnapshot(
                    symbol="test",
                    # symbol_group = tools.gen_symbol_group(contractString),
                    # asset_class = tools.gen_asset_class(symbol),
                    kind = "ORDERBOOK",

                    bids=bids,
                    asks=asks
                )
            except Exception as e:
                self._logger.error(e)
            
            # Broadcast the orderbook
            asyncio.create_task(self.broadcast(orderbook, "ORDERBOOK"))
            self._logger(f"Orderbook: {orderbook} handled")
    # ---------------------------------------
    # async def on_orderbook_received(self, tickers):
    #     for t in tickers:
            
    #         asyncio.create_task(self.broadcast(t, "ORDERBOOK"))
            # print(t)
        # orderbook = ticker
        # print(orderbook.domBids)
        
        # TODO: add something
        # orderbook.symbol = 
        # symbol = self.ezib.tickerSymbol(tickerId)
        # orderbook["symbol"] = symbol
        # orderbook["symbol_group"] = tools.gen_symbol_group(symbol)
        # orderbook["asset_class"] = tools.gen_asset_class(symbol)
        # orderbook["kind"] = "ORDERBOOK"

    # This method is implemented as async above
    
    # ---------------------------------------
    # async def on_tickers_update(self, tickers):
    #     """
    #     Handle pending tickers event - called when there are ticker updates.
    #     This is triggered by pendingTickersEvent from ib_async.
    #     """
    #     self._logger.debug(f"🔔 on_tickers_update() called with {len(tickers)} tickers")
        
    #     for i, ticker in enumerate(tickers):
    #         self._logger.debug(f"📈 Processing ticker {i+1}/{len(tickers)}")
    #         # Process each updated ticker as a quote
    #         await self.on_quote_received(ticker)
    
    # ---------------------------------------
    async def on_quote_received(self, tickerId):
        """
        Process quote updates from IB.
        
        Args:
            tickerId: Integer ticker ID from ezib's quoteUpdateEvent
        """
        try:
            # DEBUG: Log that we received a quote
            self._logger.debug(f"🔥 on_quote_received() called with tickerId: {tickerId}")
            
            # Get contract and market data from tickerId
            if not hasattr(self.ezib, 'contracts') or tickerId not in self.ezib.contracts:
                self._logger.warning(f"⚠️  No contract found for tickerId: {tickerId}")
                return
                
            contract = self.ezib.contracts[tickerId]
            
            # Get market data from ezib
            market_data = None
            if hasattr(self.ezib, 'marketData') and tickerId in self.ezib.marketData:
                market_data = self.ezib.marketData[tickerId]
            elif hasattr(self.ezib, 'optionsData') and tickerId in self.ezib.optionsData:
                market_data = self.ezib.optionsData[tickerId]
            else:
                self._logger.warning(f"⚠️  No market data found for tickerId: {tickerId}")
                return
            
            # Extract symbol from contract
            symbol = getattr(contract, 'symbol', 'UNKNOWN')
            sec_type = getattr(contract, 'secType', 'STK') 
            self._logger.info(f"📊 Processing quote for {symbol} ({sec_type}) - tickerId: {tickerId}")
            
            # DEBUG: Log available market data fields
            if hasattr(market_data, 'columns'):
                self._logger.debug(f"📊 Market data columns: {list(market_data.columns)}")
            
            # Build quote data dict from contract info
            quote_data = {
                "symbol": symbol,
                "symbol_group": tools.gen_symbol_group(symbol),
                "asset_class": tools.gen_asset_class(symbol),
                "timestamp": datetime.now(timezone.utc),
                "kind": "QUOTE"
            }
            
            # Extract market data fields (market_data is typically a DataFrame)
            if market_data is not None and len(market_data) > 0:
                # Get the latest row if it's a DataFrame
                if hasattr(market_data, 'iloc'):
                    latest_data = market_data.iloc[-1] if len(market_data) > 0 else market_data.iloc[0]
                else:
                    latest_data = market_data
                
                # Add bid/ask/last data if available
                for field in ['bid', 'ask', 'last', 'bidSize', 'askSize', 'lastSize', 'volume']:
                    value = None
                    try:
                        # For pandas Series (from DataFrame.iloc[-1]), use bracket notation
                        if hasattr(latest_data, 'index') and field in latest_data.index:
                            value = latest_data[field]
                        # For dict-like objects
                        elif hasattr(latest_data, 'get') and field in latest_data:
                            value = latest_data.get(field)
                        # For objects with attributes (but avoid pandas methods)
                        elif hasattr(latest_data, field) and not callable(getattr(latest_data, field)):
                            value = getattr(latest_data, field)
                    except (KeyError, IndexError, AttributeError):
                        continue
                        
                    if value is not None and not (isinstance(value, float) and isnan(value)):
                        if field in ['bid', 'ask', 'last']:
                            quote_data[field] = tools.to_decimal(value)
                        else:
                            # Convert numpy types to Python native types for msgpack serialization
                            if hasattr(value, 'item'):  # numpy scalar
                                quote_data[field] = value.item()
                            else:
                                quote_data[field] = value
            
            # Handle options - add opt_ prefix to Greeks
            if sec_type in ["OPT", "FOP"]:
                # Look for options-specific fields in market data
                if market_data is not None and len(market_data) > 0:
                    latest_data = market_data.iloc[-1] if hasattr(market_data, 'iloc') else market_data
                    
                    options_fields = ['delta', 'gamma', 'vega', 'theta', 'impliedVol', 'undPrice', 'dividends', 'optVolume', 'optOpenInterest', 'optPrice']
                    options_data = {}
                    
                    for field in options_fields:
                        value = None
                        try:
                            # For pandas Series (from DataFrame.iloc[-1]), use bracket notation
                            if hasattr(latest_data, 'index') and field in latest_data.index:
                                value = latest_data[field]
                            # For dict-like objects
                            elif hasattr(latest_data, 'get') and field in latest_data:
                                value = latest_data.get(field)
                            # For objects with attributes (but avoid pandas methods)
                            elif hasattr(latest_data, field) and not callable(getattr(latest_data, field)):
                                value = getattr(latest_data, field)
                        except (KeyError, IndexError, AttributeError):
                            continue
                            
                        if value is not None and not (isinstance(value, float) and isnan(value)):
                            # Convert numpy types to Python native types for msgpack serialization
                            if hasattr(value, 'item'):  # numpy scalar
                                value = value.item()
                                
                            # Map some field names
                            if field == 'impliedVol':
                                options_data["iv"] = value
                            elif field == 'undPrice':
                                options_data["underlying"] = value
                            elif field == 'dividends':
                                options_data["dividend"] = value
                            elif field == 'optVolume':
                                options_data["volume"] = value
                            elif field == 'optOpenInterest':
                                options_data["oi"] = value
                            elif field == 'optPrice':
                                options_data["price"] = value
                            else:
                                options_data[field] = value
                    
                    # Add opt_ prefix to all options fields
                    if options_data:
                        options_data = tools.mark_options_values(options_data)
                        quote_data.update(options_data)
            
            # Special handling for CASH (forex)
            if sec_type == "CASH":
                # Synthesize mid price if no last
                if (quote_data.get("last") is None and 
                    quote_data.get("bid") is not None and 
                    quote_data.get("ask") is not None):
                    quote_data["last"] = (quote_data["bid"] + quote_data["ask"]) / 2
                    
                # Check if price changed for tick generation
                if symbol in cash_ticks:
                    if cash_ticks[symbol] != quote_data.get("last"):
                        # Price changed, generate tick
                        cash_ticks[symbol] = quote_data.get("last")
                        # Note: we'd need to adapt on_tick_received to work with tickerId too
                        # await self.on_tick_received(tickerId)
                else:
                    cash_ticks[symbol] = quote_data.get("last")
            
            # DEBUG: Log quote data before broadcast
            self._logger.info(f"📡 Broadcasting QUOTE for {symbol}")
            self._logger.debug(f"📦 Quote data: {quote_data}")
            
            # Broadcast quote data
            asyncio.create_task(self.broadcast(quote_data, "QUOTE"))
            
            # DEBUG: Confirm broadcast task created
            self._logger.debug("✅ Broadcast task created")
            
            # Database logging
            if not self.args['dbskip'] and self.pool:
                asyncio.create_task(self._log_quote_to_db(quote_data))
                
        except Exception as e:
            self._logger.error(f"Error processing quote for tickerId {tickerId}: {e}")
            self.monitor.record_error("quote_processing_error", str(e))
    
    # ---------------------------------------
    async def on_tick_received(self, ticker):
        """
        Process tick data.
        
        Args:
            ticker: ib_async Ticker object with last trade info
        """
        try:
            tick_data = {
                "symbol": ticker.contract.symbol if hasattr(ticker.contract, 'symbol') else "",
                "symbol_group": tools.gen_symbol_group(ticker.contract.symbol) if hasattr(ticker.contract, 'symbol') else "",
                "asset_class": tools.gen_asset_class(ticker.contract.symbol) if hasattr(ticker.contract, 'symbol') else "STK",
                "timestamp": datetime.now(timezone.utc),
                "kind": "TICK"
            }
            
            # Get last trade info
            if hasattr(ticker, 'last') and ticker.last is not None and not isnan(ticker.last):
                tick_data["price"] = tools.to_decimal(ticker.last)
                tick_data["size"] = ticker.lastSize if hasattr(ticker, 'lastSize') else 0
                tick_data["volume"] = ticker.volume if hasattr(ticker, 'volume') else 0
                
                # Broadcast tick data
                asyncio.create_task(self.broadcast(tick_data, "TICK"))
                
                # Database logging
                if not self.args['dbskip'] and self.pool:
                    asyncio.create_task(self._log_tick_to_db(tick_data))
                    
        except Exception as e:
            self._logger.error(f"Error processing tick: {e}")
            self.monitor.record_error("tick_processing_error", str(e))
    
    # ---------------------------------------
    async def on_bar_received(self, bars, hasNewBar):
        """
        Process bar updates.
        
        Args:
            bars: RealTimeBars object from ib_async
            hasNewBar: Boolean indicating if a new bar was added
        """
        try:
            if not hasNewBar:
                return  # Only process completed bars
                
            if not bars or len(bars) == 0:
                return
                
            latest_bar = bars[-1]
            bar_data = {
                "symbol": bars.contract.symbol if hasattr(bars.contract, 'symbol') else "",
                "symbol_group": tools.gen_symbol_group(bars.contract.symbol) if hasattr(bars.contract, 'symbol') else "",
                "asset_class": tools.gen_asset_class(bars.contract.symbol) if hasattr(bars.contract, 'symbol') else "STK",
                "timestamp": latest_bar.date if hasattr(latest_bar, 'date') else datetime.now(timezone.utc),
                "open": tools.to_decimal(latest_bar.open) if hasattr(latest_bar, 'open') else 0,
                "high": tools.to_decimal(latest_bar.high) if hasattr(latest_bar, 'high') else 0,
                "low": tools.to_decimal(latest_bar.low) if hasattr(latest_bar, 'low') else 0,
                "close": tools.to_decimal(latest_bar.close) if hasattr(latest_bar, 'close') else 0,
                "volume": latest_bar.volume if hasattr(latest_bar, 'volume') else 0,
                "kind": "BAR"
            }
            
            # Broadcast bar data
            asyncio.create_task(self.broadcast(bar_data, "BAR"))
            
            # Database logging
            if not self.args['dbskip'] and self.pool:
                asyncio.create_task(self._log_bar_to_db(bar_data))
                
        except Exception as e:
            self._logger.error(f"Error processing bar: {e}")
            self.monitor.record_error("bar_processing_error", str(e))
    
    # ---------------------------------------
    async def _log_quote_to_db(self, quote_data):
        """Log quote data to database."""
        # TODO: Implement database logging for quotes
        pass
    
    # ---------------------------------------
    async def _log_tick_to_db(self, tick_data):
        """Log tick data to database."""
        # TODO: Implement database logging for ticks
        pass
    
    # ---------------------------------------
    async def _log_bar_to_db(self, bar_data):
        """Log bar data to database."""
        # TODO: Implement database logging for bars
        pass
    
    # ---------------------------------------
    async def _subscribe_market_data(self):
        """
        Subscribe to market data for all symbols in the symbols file.
        This reads the CSV and sets up real-time data subscriptions.
        """
        try:
            if not os.path.exists(self.symbols):
                self._logger.warning(f"Symbols file not found: {self.symbols}")
                return
                
            # Read symbols from CSV
            symbols_df = pd.read_csv(self.symbols)
            self._logger.info(f"Loading {len(symbols_df)} symbols from {self.symbols}")
            
            for _, row in symbols_df.iterrows():
                try:
                    # Create contract based on security type
                    contract = None
                    symbol = row.get('symbol', '')
                    sec_type = row.get('sec_type', 'STK')
                    exchange = row.get('exchange', 'SMART')
                    currency = row.get('currency', 'USD')
                    
                    # Import contract types from ib_async if available
                    try:
                        from ib_async import Stock, Option, Future, Forex, Contract
                    except ImportError:
                        self._logger.warning("ib_async contract types not available, using ezib_async")
                        continue
                    
                    if sec_type == 'STK':
                        contract = Stock(symbol, exchange, currency)
                    elif sec_type == 'OPT':
                        expiry = str(row.get('expiry', ''))
                        strike = float(row.get('strike', 0))
                        opt_type = row.get('opt_type', 'C')
                        contract = Option(symbol, expiry, strike, opt_type, exchange, currency=currency)
                    elif sec_type == 'FUT':
                        expiry = str(row.get('expiry', ''))
                        contract = Future(symbol, expiry, exchange, currency)
                    elif sec_type == 'CASH':
                        # Forex pair
                        contract = Forex(symbol)
                    else:
                        # Generic contract
                        contract = Contract()
                        contract.symbol = symbol
                        contract.secType = sec_type
                        contract.exchange = exchange
                        contract.currency = currency
                    
                    if contract:
                        # Request market data
                        ticker = self.ezib.reqMktData(contract, '', False, False)
                        
                        # Subscribe to quote updates via ticker's updateEvent
                        if hasattr(ticker, 'updateEvent'):
                            ticker.updateEvent += self.on_quote_received
                        
                        # Store ticker reference
                        self.tickers[symbol] = ticker
                        
                        # Request real-time bars if configured
                        if self.args.get('bars', False):
                            try:
                                bars = self.ezib.reqRealTimeBars(
                                    contract, 5, 'TRADES', False
                                )
                                if hasattr(bars, 'updateEvent'):
                                    bars.updateEvent += self.on_bar_received
                                self.bars[symbol] = bars
                            except Exception as e:
                                self._logger.warning(f"Could not subscribe to bars for {symbol}: {e}")
                        
                        self._logger.info(f"Subscribed to market data for {symbol}")
                        
                except Exception as e:
                    self._logger.error(f"Error subscribing to {row.get('symbol', 'unknown')}: {e}")
                    continue
                    
        except Exception as e:
            self._logger.error(f"Error in _subscribe_market_data: {e}")
            self.monitor.record_error("market_data_subscription_error", str(e))

    # -------------------------------------------
    def register(self, instruments):
        try:
            if isinstance(instruments, dict):
                instruments = list(instruments.values())

            if not isinstance(instruments, list):
                return

            db = pd.read_csv(self.symbols, header=0).fillna("")

            instruments = pd.DataFrame(instruments, columns=db.columns)
            # instruments.columns = db.columns

            # instruments['expiry'] = instruments['expiry'].astype(int).astype(str)
            # db = db.append(instruments)
            db = pd.concat([db, instruments], ignore_index=True)
            # db['expiry'] = db['expiry'].astype(int)
            key_columns = ['symbol', 'sec_type', 'exchange', 'currency', 'expiry']
            db = db.drop_duplicates(subset=key_columns, keep="first")

            db.to_csv(self.symbols, header=True, index=False)
            os.chmod(self.symbols, 0o666)

        except Exception as e:
            self._logger.error(f"Instruments should be qualified before register: {e}")

    # ---------------------------------------
    async def get_postgres_connection(self):
        """
        Get PostgreSQL connection pool using command line arguments or environment variables.
        """
        return await asyncpg.create_pool(
            host=self.args.get('dbhost', os.getenv('POSTGRES_HOST', 'localhost')),
            port=int(self.args.get('dbport', os.getenv('POSTGRES_PORT', '5432'))),
            user=self.args.get('dbuser', os.getenv('POSTGRES_USER', 'quant_async')),
            password=self.args.get('dbpass', os.getenv('POSTGRES_PASSWORD', 'quant_async')),
            database=self.args.get('dbname', os.getenv('POSTGRES_DB', 'quant_async')),
            min_size=2,
            max_size=10
        )
    
    async def postgres_connect(self):
        """
        Connect to PostgreSQL database.
        """
        if self.args['dbskip']:
            return
        
        # create asyncpg connection pool
        self.pool = await self.get_postgres_connection()
    
    async def _postgres_connect_with_monitoring(self):
        """
        Connect to PostgreSQL with monitoring and retry logic.
        """
        if self.args['dbskip']:
            self._logger.info("Database connection skipped (dbskip=True)")
            self.monitor.connection_monitor.health_checker.update_status(
                "database", "degraded", "Database connection intentionally skipped"
            )
            return

        if self.pool is not None:
            return
        
        max_retries = 5
        base_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                self._logger.info(f"Attempting database connection (attempt {attempt + 1}/{max_retries})")
                
                with self.monitor.time_operation("postgres_connect"):
                    self.pool = await self.get_postgres_connection()
                
                # Test connection
                if await self.monitor.connection_monitor.check_database_connection(self.pool, self.args['dbskip']):
                    self._logger.info("Database connection established successfully")
                    self.monitor.metrics_collector.update_connection_status("database", True)
                    return
                else:
                    raise Exception("Database connection test failed")
                    
            except Exception as e:
                error_msg = f"Database connection attempt {attempt + 1} failed: {e}"
                self._logger.warning(error_msg)
                self.monitor.record_error("database_connection_attempt", error_msg)
                
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    self._logger.info(f"Retrying database connection in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    # Final attempt failed
                    self._logger.error(f"Failed to connect to database after {max_retries} attempts")
                    self.monitor.connection_monitor.health_checker.update_status(
                        "database", "unhealthy", f"Connection failed after {max_retries} attempts"
                    )
                    # Continue without database (graceful degradation)
                    self.args['dbskip'] = True
                    self._logger.warning("Continuing in database-skip mode due to connection failures")

    async def _setup_zmq_with_monitoring(self):
        """
        Setup ZeroMQ socket with monitoring and error handling.
        """
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                self._logger.info(f"Setting up ZeroMQ socket (attempt {attempt + 1}/{max_retries})")
                
                with self.monitor.time_operation("zmq_setup"):
                    self.socket = await aiozmq.create_zmq_stream(
                        zmq.PUB, bind=f"tcp://*:{self.args['zmqport']}"
                    )
                
                # Test socket
                if await self.monitor.connection_monitor.check_zeromq_socket(self.socket):
                    self._logger.info(f"ZeroMQ broadcaster bound to: {list(self.socket.transport.bindings())}")
                    self.monitor.metrics_collector.update_connection_status("zeromq", True)
                    return
                else:
                    raise Exception("ZeroMQ socket test failed")
                    
            except Exception as e:
                error_msg = f"ZeroMQ setup attempt {attempt + 1} failed: {e}"
                self._logger.warning(error_msg)
                self.monitor.record_error("zmq_setup_attempt", error_msg)
                
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    self._logger.info(f"Retrying ZeroMQ setup in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
                else:
                    # Final attempt failed - this is critical
                    self._logger.error(f"Failed to setup ZeroMQ after {max_retries} attempts")
                    self.monitor.connection_monitor.health_checker.update_status(
                        "zeromq", "unhealthy", f"Setup failed after {max_retries} attempts"
                    )
                    raise Exception(f"ZeroMQ setup failed after {max_retries} attempts")
    
    async def _connect_ib_with_retry(self):
        """
        Connect to Interactive Brokers with enhanced retry logic and monitoring.
        """
        max_retries = 10
        base_delay = 2.0
        max_delay = 30.0
        
        self._logger.info(f"Connecting to Interactive Brokers at {self.args['ibhost']}:{self.args['ibport']} (client: {self.args['ibclient']})")
        
        # Initialize IB connection
        self.ezib = ezIBAsync()
        self._register_events_handler()
        
        for attempt in range(max_retries):
            try:
                self._logger.info(f"IB connection attempt {attempt + 1}/{max_retries}")
                
                with self.monitor.time_operation("ib_connect"):
                    await self.ezib.connectAsync(
                        ibhost=str(self.args['ibhost']),
                        ibport=int(self.args['ibport']),
                        ibclient=int(self.args['ibclient'])
                    )
                
                # Check connection with monitoring
                if await self.monitor.connection_monitor.check_ib_connection(self.ezib):
                    self._logger.info("Interactive Brokers connection established successfully")
                    self.monitor.metrics_collector.update_connection_status("ib", True)
                    return
                else:
                    raise Exception("IB connection test failed")
                    
            except Exception as e:
                error_msg = f"IB connection attempt {attempt + 1} failed: {e}"
                self._logger.warning(error_msg)
                self.monitor.record_error("ib_connection_attempt", error_msg)
                
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter and max delay
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    # Add jitter to prevent thundering herd
                    import random
                    jitter = random.uniform(0.8, 1.2)
                    delay *= jitter
                    
                    self._logger.info(f"Retrying IB connection in {delay:.1f} seconds...")
                    print('*', end="", flush=True)  # Keep original progress indicator
                    await asyncio.sleep(delay)
                else:
                    # Final attempt failed - this is critical
                    self._logger.error(f"Failed to connect to Interactive Brokers after {max_retries} attempts")
                    self.monitor.connection_monitor.health_checker.update_status(
                        "interactive_brokers", "unhealthy", f"Connection failed after {max_retries} attempts"
                    )
                    raise Exception(f"IB connection failed after {max_retries} attempts")

    # ---------------------------------------        
    async def run(self):
        """
        Start the blotter with enhanced monitoring and error handling.

        Connects to the TWS/GW and sets up the IB connection with retry logic,
        monitoring, and graceful degradation.
        """
        
        try:
            # Start monitoring system
            await self.monitor.start_monitoring(self)
            self._logger.info("System monitoring started")
            
            # Check for unique blotter instance
            await self._check_unique_blotter()

            # Connect to postgres with monitoring
            await self._postgres_connect_with_monitoring()
            
            # Setup ZeroMQ with monitoring
            await self._setup_zmq_with_monitoring()
            
            # Connect to IB with enhanced retry logic
            await self._connect_ib_with_retry()
            
            # Subscribe to market data for symbols
            # await self._subscribe_market_data()

            # Start watching symbols file
            asyncio.create_task(self._watch_symbols_file())
            
            # Create an event that will never be set
            self.stop_event = asyncio.Event()
            
            # Setup signal handlers for graceful shutdown
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))
            
            self._logger.info("Blotter fully operational - all systems ready")
            
            # Wait for the event (which will never be set unless we call _shutdown)
            await self.stop_event.wait()

        except asyncio.CancelledError:
            self._logger.info("Blotter cancelled - performing graceful shutdown")
        except Exception as e:
            error_msg = f"Critical error in blotter run: {e}"
            self._logger.error(error_msg)
            self.monitor.record_error("blotter_run_error", error_msg)
            raise
        finally:
            # Cleanup with monitoring
            await self._cleanup_with_monitoring()
    
    async def stream(self, symbols, tick_handler=None, bar_handler=None,
               quote_handler=None, book_handler=None, tz="UTC"):
        """
        Stream market data asynchronously using aiozmq.
        
        Args:
            symbols (list or str): Symbols to subscribe to
            tick_handler (callable, optional): Handler for tick data
            bar_handler (callable, optional): Handler for bar data
            quote_handler (callable, optional): Handler for quote data
            book_handler (callable, optional): Handler for orderbook data
            tz (str, optional): Timezone for datetime conversion. Defaults to "UTC".
        """
        # Load runtime/default data
        if isinstance(symbols, str):
            symbols = symbols.split(',')
        symbols = list(map(str.strip, symbols))
        
        # Create ZMQ SUB socket
        socket = await aiozmq.create_zmq_stream(
            zmq.SUB, 
            connect=f'tcp://127.0.0.1:{self.args["zmqport"]}'
        )
        
        # Subscribe to all messages
        socket.transport.setsockopt(zmq.SUBSCRIBE, b'')
        
        self._logger.info(f"Streaming data for symbols: {symbols}")
        while True:
            try:
                # Wait for message
                msg = await asyncio.wait_for(socket.read(), timeout=5)
                data = msgspec.msgpack.decode(msg[0], type=OrderBookSnapshot)
                # print(data)
                
                if data.kind == "ORDERBOOK":
                    if book_handler is not None:
                        asyncio.create_task(book_handler(data))
                        continue
                
                # if self.args["zmqtopic"] in message:
                #     message = message.split(self.args["zmqtopic"])[1].strip()
                #     data = json.loads(message)
                    
                #     if data['symbol'] not in symbols:
                #         continue
                    
                #     # Convert None to np.nan
                #     for k, v in data.items():
                #         if v is None:
                #             data[k] = float('nan')
                    
                #     # Handle orderbook data
                #     if data['kind'] == "ORDERBOOK":
                #         print("orderbook data...")
                #         if book_handler is not None:
                #             asyncio.create_task(book_handler(data))
                #             continue
                    
                #     # Handle quote data
                #     if data['kind'] == "QUOTE":
                #         if quote_handler is not None:
                #             asyncio.create_task(quote_handler(data))
                #             continue
                    
                #     # Process tick and bar data
                #     try:
                #         if 'timestamp' in data:
                #             data["datetime"] = pd.to_datetime(data["timestamp"])
                #         else:
                #             data["datetime"] = pd.Timestamp.now(tz='UTC')
                #     except Exception as e:
                #         self._logger.error(f"Error parsing timestamp: {e}")
                #         data["datetime"] = pd.Timestamp.now(tz='UTC')
                    
                #     # Create DataFrame
                #     df = pd.DataFrame(index=[0], data=data)
                #     df.set_index('datetime', inplace=True)
                #     df.index = pd.to_datetime(df.index, utc=True)
                    
                #     # Drop unnecessary columns
                #     if "timestamp" in df.columns:
                #         df.drop("timestamp", axis=1, inplace=True)
                #     if "kind" in df.columns:
                #         df.drop("kind", axis=1, inplace=True)
                    
                #     # Convert timezone
                #     try:
                #         df.index = df.index.tz_convert(tz)
                #     except Exception as e:
                #         try:
                #             df.index = df.index.tz_localize('UTC').tz_convert(tz)
                #         except Exception as e:
                #             self._logger.error(f"Error converting timezone: {e}")
                    
                #     # Handle tick data
                #     if data['kind'] == "TICK":
                #         if tick_handler is not None:
                #             tick_handler(df)
                    
                #     # Handle bar data
                #     elif data['kind'] == "BAR":
                #         if bar_handler is not None:
                #             bar_handler(df)
            
            except asyncio.CancelledError:
                self._logger.info("Stream task cancelled")
                socket.close()
                await socket.drain()
                raise
            except asyncio.TimeoutError:
                continue
            
            except Exception as e:
                self._logger.error(f"Error in stream: {e}")
                if socket:
                    socket.close()
                    await socket.drain()
                raise

    # ---------------------------------------
    # Historical Data Management Methods
    # ---------------------------------------
    
    async def history(self, symbols, start, end=None, resolution='1T', tz='UTC'):
        """
        Retrieve historical market data from PostgreSQL.
        
        Args:
            symbols (list or str): List of symbols to query
            start (str or datetime): Start date/time for data retrieval
            end (str or datetime, optional): End date/time. Defaults to now.
            resolution (str, optional): Time frequency ('1T', '5T', '1H', '1D'). Defaults to '1T'.
            tz (str, optional): Timezone for datetime conversion. Defaults to 'UTC'.
            
        Returns:
            pandas.DataFrame: Historical market data with OHLCV columns
            
        Raises:
            RuntimeError: If database is not connected
            ValueError: If invalid symbols or date range provided
        """
        import pandas as pd
        from datetime import datetime
        
        # Validate inputs
        if isinstance(symbols, str):
            symbols = [symbols]
        
        if not symbols:
            raise ValueError("At least one symbol must be provided")
            
        # Parse date range
        if isinstance(start, str):
            start = pd.to_datetime(start)
        if isinstance(end, str):
            end = pd.to_datetime(end)
        elif end is None:
            end = datetime.now()
            
        # Validate database connection
        if not self.pool:
            raise RuntimeError("Database not connected")
            
        self._logger.info(f"Querying history for {symbols} from {start} to {end}")
        
        try:
            async with self.pool.acquire() as conn:
                # CRITICAL: Use prepared statement for performance
                query = """
                    SELECT 
                        s.symbol,
                        b.datetime,
                        b.open,
                        b.high,
                        b.low,
                        b.close,
                        b.volume
                    FROM bars b 
                    JOIN symbols s ON b.symbol_id = s.id
                    WHERE s.symbol = ANY($1) 
                        AND b.datetime BETWEEN $2 AND $3
                    ORDER BY s.symbol, b.datetime
                """
                
                # PATTERN: Use existing monitoring integration
                with self.monitor.time_operation("history_query"):
                    rows = await conn.fetch(query, symbols, start, end)
                
                # Convert to pandas DataFrame for qtpylib compatibility
                if not rows:
                    self._logger.warning(f"No data found for symbols {symbols}")
                    return pd.DataFrame()
                
                df = pd.DataFrame(rows)
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                
                # Convert timezone if requested
                if tz != 'UTC':
                    target_tz = pytz.timezone(tz)
                    df['datetime'] = df['datetime'].dt.tz_convert(target_tz)
                
                df.set_index('datetime', inplace=True)
                
                self._logger.info(f"Retrieved {len(df)} rows of historical data")
                return df
                
        except Exception as e:
            error_msg = f"History query failed: {e}"
            self._logger.error(error_msg)
            if hasattr(self, 'monitor'):
                self.monitor.record_error("history_query_error", error_msg)
            raise
    
    async def backfill(self, symbols, start, end, resolution='1T'):
        """
        Backfill historical data from Interactive Brokers.
        
        Args:
            symbols (list or str): List of symbols to backfill
            start (str or datetime): Start date for backfill
            end (str or datetime): End date for backfill  
            resolution (str, optional): Time frequency. Defaults to '1T'.
            
        Returns:
            int: Number of bars retrieved and stored
            
        Raises:
            RuntimeError: If IB is not connected or database unavailable
        """
        if isinstance(symbols, str):
            symbols = [symbols]
            
        if not self.ezib or not self.ezib.connected:
            raise RuntimeError("Interactive Brokers not connected")
            
        if not self.pool:
            raise RuntimeError("Database not connected")
            
        self._logger.info(f"Starting backfill for {symbols} from {start} to {end}")
        
        total_bars = 0
        
        try:
            for symbol in symbols:
                # Request historical data from IB
                # Note: This is a placeholder - actual IB historical data request
                # would need to be implemented based on ezib_async API
                self._logger.info(f"Requesting historical data for {symbol}")
                
                # For now, log the backfill request
                # TODO: Implement actual IB historical data request
                # bars = await self.ezib.reqHistoricalData(symbol, start, end, resolution)
                # await self.storage.save_bars_batch(bars)
                
                self._logger.warning(f"Backfill for {symbol} not yet implemented - requires IB API integration")
                
            return total_bars
            
        except Exception as e:
            error_msg = f"Backfill failed: {e}"
            self._logger.error(error_msg)
            if hasattr(self, 'monitor'):
                self.monitor.record_error("backfill_error", error_msg)
            raise

    # ---------------------------------------
    async def _shutdown(self):
        """Handle graceful shutdown with enhanced monitoring."""
        self._logger.info("Shutdown signal received - initiating graceful shutdown...")
        print("\n\n>>> Interrupted with Ctrl-c...\n(waiting for running tasks to be completed)\n")
        
        try:
            # Record shutdown event
            if hasattr(self, 'monitor'):
                self.monitor.record_error("shutdown_initiated", "Graceful shutdown started")
            
            # Set stop event to break main loop
            if hasattr(self, 'stop_event'):
                self.stop_event.set()
            
            # Cancel all running tasks except the current one
            current_task = asyncio.current_task()
            tasks_to_cancel = []
            
            for task in asyncio.all_tasks():
                if task is not current_task and not task.done():
                    tasks_to_cancel.append(task)
                    task.cancel()
            
            if tasks_to_cancel:
                self._logger.info(f"Cancelling {len(tasks_to_cancel)} running tasks...")
                # Wait for tasks to be cancelled with timeout
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                        timeout=10.0  # 10 second timeout for graceful shutdown
                    )
                except asyncio.TimeoutError:
                    self._logger.warning("Some tasks did not complete within shutdown timeout")
            
            self._logger.info("Graceful shutdown completed")
            
        except Exception as e:
            self._logger.error(f"Error during shutdown: {e}")
            if hasattr(self, 'monitor'):
                self.monitor.record_error("shutdown_error", str(e))
    
    # ---------------------------------------
    async def _cleanup(self):
        """Clean up resources."""
        # Clean up ZMQ socket
        # if self.socket:
        #     await self.socket.close()
        #     self.socket = None
        
        # Disconnect from IB
        if self.ezib.connected:
            # self.ezib.cancelMarketData()
            self.ezib.disconnect()
            self._logger.info("Dconnected from IB")
            
        # Remove cached args file
        await self._remove_cached_args()
    
    async def _cleanup_with_monitoring(self):
        """
        Enhanced cleanup with monitoring and graceful resource management.
        """
        self._logger.info("Starting cleanup process...")
        
        try:
            # Stop monitoring system first
            if hasattr(self, 'monitor'):
                self._logger.info("Stopping monitoring system...")
                await self.monitor.stop_monitoring()
            
            # Clean up ZMQ socket with error handling
            if self.socket:
                try:
                    self._logger.info("Closing ZeroMQ socket...")
                    with self.monitor.time_operation("zmq_cleanup"):
                        self.socket.close()
                        await self.socket.drain()
                    self.socket = None
                    self.monitor.metrics_collector.update_connection_status("zeromq", False)
                    self._logger.info("ZeroMQ socket closed successfully")
                except Exception as e:
                    self._logger.warning(f"Error closing ZeroMQ socket: {e}")
                    self.monitor.record_error("zmq_cleanup_error", str(e))
            
            # Disconnect from IB with error handling
            if hasattr(self, 'ezib') and self.ezib.connected:
                try:
                    self._logger.info("Disconnecting from Interactive Brokers...")
                    with self.monitor.time_operation("ib_disconnect"):
                        # Cancel all market data subscriptions
                        # self.ezib.cancelMarketData()  # Uncomment if needed
                        self.ezib.disconnect()
                    self.monitor.metrics_collector.update_connection_status("ib", False)
                    self._logger.info("Disconnected from Interactive Brokers successfully")
                except Exception as e:
                    self._logger.warning(f"Error disconnecting from IB: {e}")
                    self.monitor.record_error("ib_disconnect_error", str(e))
            
            # Close Postgres connection pool with error handling
            if self.pool:
                try:
                    self._logger.info("Closing database connection pool...")
                    with self.monitor.time_operation("db_cleanup"):
                        await self.pool.close()
                    self.pool = None
                    self.monitor.metrics_collector.update_connection_status("database", False)
                    self._logger.info("Database connection pool closed successfully")
                except Exception as e:
                    self._logger.warning(f"Error closing database pool: {e}")
                    self.monitor.record_error("db_cleanup_error", str(e))
            
            # Remove cached args file
            try:
                await self._remove_cached_args()
                self._logger.info("Cached arguments file removed")
            except Exception as e:
                self._logger.warning(f"Error removing cached args: {e}")
            
            self._logger.info("Cleanup process completed successfully")
            
        except Exception as e:
            self._logger.error(f"Critical error during cleanup: {e}")
            if hasattr(self, 'monitor'):
                self.monitor.record_error("cleanup_critical_error", str(e))
    
    # =======================================
    # DataStore Integration Methods
    # =======================================
    
    async def _init_datastore(self):
        """Initialize DataStore for enhanced data operations"""
        try:
            if not self.args.get('dbskip') and self.pool:
                from .storage import DataStore
                self.datastore = DataStore(self.pool)
                self._logger.info("DataStore initialized successfully")
                return True
        except Exception as e:
            self._logger.error(f"Failed to initialize DataStore: {e}")
            self.monitor.record_error("datastore_init_error", str(e))
        return False
    
    async def history(self, symbols=None, start=None, end=None, resolution='1T'):
        """
        Enhanced history method with DataStore integration.
        
        Args:
            symbols (list): List of symbols to retrieve
            start (str): Start datetime string
            end (str): End datetime string  
            resolution (str): Data resolution (not used in current implementation)
            
        Returns:
            pandas.DataFrame: Historical bar data with MultiIndex (symbol, datetime)
        """
        try:
            if not self.datastore:
                await self._init_datastore()
                
            if not self.datastore:
                self._logger.warning("DataStore not available, returning empty DataFrame")
                return pd.DataFrame()
            
            # Parse datetime strings and convert to naive datetime for database compatibility
            start_dt = pd.to_datetime(start, utc=True).to_pydatetime().replace(tzinfo=None)
            end_dt = pd.to_datetime(end, utc=True).to_pydatetime().replace(tzinfo=None)
            
            # Use DataStore to retrieve historical data
            with self.monitor.time_operation("history_retrieval"):
                df = await self.datastore.get_historical_bars(symbols, start_dt, end_dt)
                
            self._logger.info(f"Retrieved {len(df)} historical bars for symbols: {symbols}")
            self.monitor.record_operation("history_success", {"symbols": len(symbols or []), "rows": len(df)})
            
            return df
            
        except Exception as e:
            self._logger.error(f"History retrieval failed: {e}")
            self.monitor.record_error("history_error", str(e))
            return pd.DataFrame()
    
    async def backfill(self, symbol=None, start=None, end=None, resolution='1T'):
        """
        Enhanced backfill method with DataStore integration.
        
        Args:
            symbol (str): Symbol to backfill
            start (str): Start datetime string
            end (str): End datetime string
            resolution (str): Data resolution (not used in current implementation)
            
        Returns:
            pandas.DataFrame or None: Backfilled data or None if not available
        """
        try:
            if not self.datastore:
                await self._init_datastore()
                
            if not self.datastore:
                self._logger.warning("DataStore not available for backfill")
                return None
            
            # Parse datetime strings and convert to naive datetime for database compatibility
            start_dt = pd.to_datetime(start, utc=True).to_pydatetime().replace(tzinfo=None)
            end_dt = pd.to_datetime(end, utc=True).to_pydatetime().replace(tzinfo=None)
            
            # For demo purposes, we'll try to retrieve existing data
            # In a real implementation, this would connect to a data provider
            with self.monitor.time_operation("backfill_operation"):
                df = await self.datastore.get_historical_bars([symbol], start_dt, end_dt)
                
            if not df.empty:
                self._logger.info(f"Backfill found {len(df)} existing bars for {symbol}")
                self.monitor.record_operation("backfill_success", {"symbol": symbol, "rows": len(df)})
                return df
            else:
                self._logger.info(f"No existing data found for backfill of {symbol}")
                self.monitor.record_operation("backfill_no_data", {"symbol": symbol})
                return None
                
        except Exception as e:
            self._logger.error(f"Backfill failed for {symbol}: {e}")
            self.monitor.record_error("backfill_error", str(e))
            return None

# ===========================================
#  Utility functions 
# ===========================================
def load_blotter_args(blotter_name=None, logger=None):
    """ Load running blotter's settings (used by clients)

    :Parameters:
        blotter_name : str
            Running Blotter's name (defaults to "auto-detect")
        logger : object
            Logger to be use (defaults to Blotter's)

    :Returns:
        args : dict
            Running Blotter's arguments
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # find specific name
    if blotter_name is not None:  # and blotter_name != 'auto-detect':
        args_cache_file = tempfile.gettempdir() + "/" + blotter_name.lower() + ".quant_async"
        if not os.path.exists(args_cache_file):
            logger.critical(
                "Cannot connect to running Blotter [%s]", blotter_name)
            if os.isatty(0):
                sys.exit(0)
            return {}

    # no name provided - connect to last running
    else:
        blotter_files = sorted(
            glob.glob(tempfile.gettempdir() + "/*.quant_async"), key=os.path.getmtime)

        if not blotter_files:
            logger.critical(
                "Cannot connect to running Blotter [%s]", blotter_name)
            if os.isatty(0):
                sys.exit(0)
            return {}

        args_cache_file = blotter_files[-1]

    args = pickle.load(open(args_cache_file, "rb"))
    args['as_client'] = True

    return args

if __name__ == "__main__":
    blotter = Blotter()
    asyncio.run(blotter.run())