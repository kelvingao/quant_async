import signal
import argparse
import asyncio
import logging

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
        **kwargs: Additional keyword arguments.
    """

    def __init__(self, ib_host="localhost", ib_port=4001, clientId=999, **kwargs):
        # Initialize class logger
        self.logger = logging.getLogger("quant_async.blotter")
        
        # IB connection parameters
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.clientId = clientId
        
        # Store arguments
        self.args = {arg: val for arg, val in locals().items()
                    if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())
        
        # Initialize IB connection
        self.ibConn = ezIBAsync()
    
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
       
            
if __name__ == "__main__":
    blotter = Blotter()
    asyncio.run(blotter.run())