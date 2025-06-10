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
import argparse
import asyncio
import logging
import sys
import os
import tempfile
import pickle
import pandas as pd
from datetime import datetime

# Import ezib_async
from ezib_async import ezIBAsync

path = {
    "library": os.path.dirname(os.path.realpath(__file__)),
    "caller": os.path.dirname(os.path.realpath(sys.argv[0]))
}

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
                 symbols="symbols.csv", orderbook=False, zmqport="12345", zmqtopic=None,**kwargs):
        # Initialize class logger
        self._logger = logging.getLogger("quant_async.blotter")
        
        # Set blotter name
        self.name = str(self.__class__).split('.')[-1].split("'")[0].lower()
        if name is not None:
            self.name = name
        
        # IB connection parameters
        self.ibhost = ibhost
        self.ibport = ibport
        self.ibclient = ibclient
        
        # Store arguments
        self.args = {arg: val for arg, val in locals().items()
                    if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())

        # Symbols file and monitoring settings
        self.symbols = path['caller'] + '/' + self.args['symbols']
        
        # Initialize args cache file path
        self.args_cache_file = os.path.join(tempfile.gettempdir(), f"{self.name}.quant_async")
        
        # Flag to track if this is a duplicate run
        self.duplicate_run = False
        
        # Initialize IB connection
        self.ezib = ezIBAsync()
    
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
            self._logger.error(f"Error checking if blotter is running: {e}")
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
        self._logger.debug(f"Starting to watch symbols file: {self.symbols}")
        
        # Initialize tracking variables
        first_run = True
        prev_contracts = []
        db_modified = 0
        
        while True:
            try:
                # Check if file exists
                if not os.path.exists(self.symbols):
                    self._logger.info(f"Creating symbols file: {self.symbols}")
                    # Create empty symbols file with required columns
                    df = pd.DataFrame(columns=['symbol', 'secType', 'exchange', 
                                             'currency', 'expiry', 'strike', 'right'])
                    df.to_csv(self.symbols, index=False)
                    os.chmod(self.symbols, 0o666)  # Make writable by all
                else:
                    # Small delay before checking file stats
                    await asyncio.sleep(0.1)
                    
                    # Read file stats
                    db_data = os.stat(self.symbols)
                    db_size = db_data.st_size
                    db_last_modified = db_data.st_mtime
                    
                    # Handle empty file
                    if db_size == 0:
                        if prev_contracts:
                            self._logger.info('Empty symbols file, canceling all market data...')
                            for contract in prev_contracts:
                                contract_obj = self.ezib.createContract(*contract)
                                await self.ezib.cancelMarketData(contract_obj)
                                if self.orderbook:
                                    await self.ezib.cancelMarketDepth(contract_obj)
                            prev_contracts = []
                        await asyncio.sleep(1)
                        continue
                    
                    # Check if file was modified
                    if not first_run and db_last_modified == db_modified:
                        await asyncio.sleep(1)
                        continue
                    
                    # Update modification time
                    db_modified = db_last_modified
                    
                    # Read symbols file
                    df = pd.read_csv(self.symbols)
                    if df.empty:
                        await asyncio.sleep(1)
                        continue
                    
                    # Process the data
                    
                    # Remove expired contracts
                    current_month = int(datetime.now().strftime('%Y%m'))
                    current_day = int(datetime.now().strftime('%Y%m%d'))
                    
                    # Filter based on expiry dates
                    if 'expiry' in df.columns:
                        # Convert expiry to numeric for comparison
                        df['expiry'] = pd.to_numeric(df['expiry'], errors='coerce')
                        
                        # Filter out expired contracts
                        mask = (
                            pd.isna(df['expiry']) |  # Keep contracts with no expiry
                            ((df['expiry'] < 1000000) & (df['expiry'] >= current_month)) |  # YYYYMM format
                            ((df['expiry'] >= 1000000) & (df['expiry'] >= current_day))  # YYYYMMDD format
                        )
                        df = df[mask]
                        
                        # Convert expiry back to string format
                        df['expiry'] = df['expiry'].fillna(0).astype(int).astype(str)
                        df.loc[df['expiry'] == "0", 'expiry'] = ""
                    
                    # Filter out BAG security types
                    if 'secType' in df.columns:
                        df = df[df['secType'] != 'BAG']
                    
                    # Fill NaN values
                    df.fillna("", inplace=True)
                    
                    # Save cleaned data back to file
                    df.to_csv(self.symbols, index=False)
                    os.chmod(self.symbols, 0o666)  # Make writable by all
                    
                    # Filter out commented symbols
                    df = df[~df['symbol'].astype(str).str.startswith('#')]
                    
                    # Convert to list of tuples
                    contracts = [tuple(x) for x in df.values]
                    
                    if first_run:
                        first_run = False
                        # Request market data for all contracts on first run
                        for contract in contracts:
                            contract_obj = self.ezib.createContract(*contract)
                            await self.ezib.requestMarketData(contract_obj)
                            if self.orderbook:
                                await self.ezib.requestMarketDepth(contract_obj)
                            
                            # Log the addition
                            contract_string = str(contract[0])  # Use symbol as identifier
                            self._logger.info(f'Contract Added [{contract_string}]')
                    else:
                        # Cancel market data for removed contracts
                        if contracts != prev_contracts:
                            for contract in prev_contracts:
                                if contract not in contracts:
                                    contract_obj = self.ezib.createContract(*contract)
                                    await self.ezib.cancelMarketData(contract_obj)
                                    if self.orderbook:
                                        await self.ezib.cancelMarketDepth(contract_obj)
                                    
                                    # Log the removal
                                    contract_string = str(contract[0])  # Use symbol as identifier
                                    self._logger.info(f'Contract Removed [{contract_string}]')
                    
                            # Request market data for new contracts
                            for contract in contracts:
                                if contract not in prev_contracts:
                                    contract_obj = self.ezib.createContract(*contract)
                                    await self.ezib.requestMarketData(contract_obj)
                                    if self.orderbook:
                                        await self.ezib.requestMarketDepth(contract_obj)
                                    
                                    # Log the addition
                                    contract_string = str(contract[0])  # Use symbol as identifier
                                    self._logger.info(f'Contract Added [{contract_string}]')
                    
                    # Update previous contracts list
                    prev_contracts = contracts
                
                # Wait before next check
                await asyncio.sleep(1)
                
            except Exception as e:
                self._logger.error(f"Error watching symbols file: {e}")
                await asyncio.sleep(1)
    
    # ---------------------------------------
    async def ibCallback(self, caller, msg, **kwargs):
        """
        Callback for Interactive Brokers events.
        
        Args:
            caller (str): Caller name.
            msg: Message object.
            **kwargs: Additional keyword arguments.
        """
        pass

    # ---------------------------------------        
    async def run(self):
        """
        Start the blotter.

        Connects to the TWS/GW and sets up the IB connection.
        """
        
        # Set callback
        self.ezib.callback = self.ibCallback
        
        try:
            # Check for unique blotter instance
            await self._check_unique_blotter()
            
            self._logger.info("Connecting to Interactive Brokers...")
            # Connect to IB
            while not self.ezib.isConnected:
                await self.ezib.connectAsync(
                    ibhost=self.args['ibhost'],
                    ibport=self.args['ibport'],
                    ibclient=self.args['ibclient']
                )
                await asyncio.sleep(2)

                if not self.ezib.isConnected:
                    print('*', end="", flush=True)
            self._logger.info(f"Connection established to IB at {self.args['ibhost']}:{self.args['ibport']}")

            # Start watching symbols file
            asyncio.create_task(self._watch_symbols_file())
            
            # Create an event that will never be set
            stop_event = asyncio.Event()
            
            # Setup signal handlers for graceful shutdown
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self._shutdown()))
            
            # Wait for the event (which will never be set unless we call _shutdown)
            await stop_event.wait()

        except asyncio.CancelledError:
            # This is expected when Ctrl+C is pressed
            pass
        except Exception as e:
            self._logger.error(f"Error: {e}")
        finally:
            # Cleanup
            await self._cleanup()
    
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
        """Clean up resources."""
        # Disconnect from IB
        if self.ezib.isConnected:
            self.ezib.disconnect()
            self._logger.info("Disconnected from IB")
            
        # Remove cached args file
        await self._remove_cached_args()

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
    # if logger is None:
    #     logger = tools.createLogger(__name__, logging.WARNING)

    # find specific name
    if blotter_name is not None:  # and blotter_name != 'auto-detect':
        args_cache_file = tempfile.gettempdir() + "/" + blotter_name.lower() + ".quant_async"
        if not os.path.exists(args_cache_file):
            logger.critical(
                "Cannot connect to running Blotter [%s]", blotter_name)
            if os.isatty(0):
                sys.exit(0)
            return []

    # no name provided - connect to last running
    else:
        blotter_files = sorted(
            glob.glob(tempfile.gettempdir() + "/*.quant_async"), key=os.path.getmtime)

        if not blotter_files:
            logger.critical(
                "Cannot connect to running Blotter [%s]", blotter_name)
            if os.isatty(0):
                sys.exit(0)
            return []

        args_cache_file = blotter_files[-1]

    args = pickle.load(open(args_cache_file, "rb"))
    args['as_client'] = True

    return args

if __name__ == "__main__":
    blotter = Blotter()
    asyncio.run(blotter.run())