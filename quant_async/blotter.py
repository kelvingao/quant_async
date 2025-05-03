import signal
import argparse
import asyncio
import logging
import sys
import os
import tempfile
import pickle

# Import ezib_async
from ezib_async import ezIBAsync

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class Blotter:
    """
    Blotter class for handling Interactive Brokers connections and market data.
    
    This class manages the connection to Interactive Brokers, processes and logs market data.
    
    Args:
        ib_host (str, optional): IB TWS/GW Server hostname. Defaults to "localhost".
        ib_port (int, optional): TWS/GW Port to use. Defaults to 4001.
        clientId (int, optional): TWS/GW Client ID. Defaults to 999.
        name (str, optional): Name of the blotter instance. Defaults to class name.
        **kwargs: Additional keyword arguments.
    """

    def __init__(self, ib_host="localhost", ib_port=4001, clientId=999, name=None, **kwargs):
        # Initialize class logger
        self.logger = logging.getLogger("quant_async.blotter")
        
        # Set blotter name
        self.name = str(self.__class__).split('.')[-1].split("'")[0].lower()
        if name is not None:
            self.name = name
        
        # IB connection parameters
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.clientId = clientId
        
        # Store arguments
        self.args = {arg: val for arg, val in locals().items()
                    if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())
        
        # Initialize args cache file path
        self.args_cache_file = os.path.join(tempfile.gettempdir(), f"{self.name}.quant_async")
        
        # Flag to track if this is a duplicate run
        self.duplicate_run = False
        
        # Initialize IB connection
        self.ibConn = ezIBAsync()
    
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
            logging.error(f"Error checking if blotter is running: {e}")
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
                    self.logger.error(f"Blotter '{self.name}' is already running...")
                    sys.exit(1)
            
            # Write current args to cache file
            await self._write_cached_args()
            self.logger.info(f"Started Blotter instance: {self.name}")
            
        except Exception as e:
            self.logger.error(f"Error checking unique blotter: {e}")
            sys.exit(1)
    
    # ---------------------------------------
    async def _remove_cached_args(self):
        """Remove cached arguments file."""
        if os.path.exists(self.args_cache_file):
            try:
                os.remove(self.args_cache_file)
                self.logger.debug(f"Removed cached args file: {self.args_cache_file}")
            except Exception as e:
                self.logger.error(f"Error removing cached args file: {e}")
    
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
                self.logger.error(f"Error reading cached args: {e}")
        return {}
    
    # ---------------------------------------
    async def _write_cached_args(self):
        """Write arguments to cache file."""
        try:
            with open(self.args_cache_file, 'wb') as f:
                pickle.dump(self.args, f)
            
            # Set file permissions
            os.chmod(self.args_cache_file, 0o666)
            self.logger.debug(f"Wrote cached args to: {self.args_cache_file}")
        except Exception as e:
            self.logger.error(f"Error writing cached args: {e}")
    
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
            
        parser.add_argument('--ib-host', default=self.args['ib_host'],
                          help='IB TWS/GW Server hostname', required=False)
        parser.add_argument('--ib-port', default=self.args['ib_port'],
                          help='TWS/GW Port to use', required=False)
        parser.add_argument('--clientId', default=self.args['clientId'],
                          help='TWS/GW Client ID', required=False)
                          
        # Only return non-default cmd line args
        # (meaning only those actually given)
        cmd_args, _ = parser.parse_known_args()
        args = {arg: val for arg, val in vars(
            cmd_args).items() if val != parser.get_default(arg)}
        return args
    
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
        self.logger.info("Connecting to Interactive Brokers...")
        
        # Set callback
        self.ibConn.callback = self.ibCallback
        
        try:
            # Check for unique blotter instance
            await self._check_unique_blotter()
            
            # Connect to IB
            await self.ibConn.connectAsync(
                host=self.args['ib_host'],
                port=self.args['ib_port'],
                clientId=self.args['clientId']
            )
            self.logger.info(f"Connection established to IB at {self.args['ib_host']}:{self.args['ib_port']}")

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
            self.logger.error(f"Error: {e}")
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
        if self.ibConn.isConnected:
            await self.ibConn.disconnect()
            self.logger.info("Disconnected from IB")
        # Remove cached args file
        await self._remove_cached_args()
       
            
if __name__ == "__main__":
    blotter = Blotter()
    asyncio.run(blotter.run())