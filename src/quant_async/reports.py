import asyncio
import argparse
import hashlib
import datetime
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from contextlib import asynccontextmanager

# Initialize the Interactive Brokers client
from ezib_async import ezIBAsync


class Reports:
    
    def __init__(self, ibhost='127.0.0.1', ibport=4001, ibclient=0, 
                 static_dir=None, templates_dir=None, password=None, **kwargs):
        """
        Initialize the Reports class.
        
        Args:
            ibhost (str): IB Gateway/TWS host address
            ibport (int): IB Gateway/TWS port
            ibclient (int): Client ID for IB connection
            static (Path or str, optional): Directory for static files. 
                Defaults to _webapp/static in the package directory.
            templates (Path or str, optional): Directory for templates. 
                Defaults to _webapp/templates in the package directory.
            password (str, optional): Password for authentication. 
                Defaults to a hash of the current date.
            **kwargs: Additional keyword arguments.
        """

        # initialize the Interactive Brokers client
        self.ezib = ezIBAsync()
        self.app = None
        
        # IB connection parameters
        self.ibhost = ibhost
        self.ibport = ibport
        self.ibclient = ibclient

        # override args with any (non-default) command-line args
        self.args = {arg: val for arg, val in locals().items()
                    if arg not in ('__class__', 'self', 'kwargs')}
        self.args.update(kwargs)
        self.args.update(self.load_cli_args())
        
        # web application directories
        self.static_dir = (Path(static_dir)
            if static_dir else Path(__file__).parent / "_webapp")   
        self.templates_dir = (Path(templates_dir)
            if templates_dir else Path(__file__).parent / "_webapp")
        
        # return
        self._password = password if password is not None else hashlib.sha1(
            str(datetime.datetime.now().date()).encode('utf-8')).hexdigest()[:8]

        self._logger = logging.getLogger('quant_async.reports')
    
    # ---------------------------------------
    def setup_app(self):
        """
        Set up the FastAPI application.
        """
        # Create FastAPI app with lifespan
        self.app = FastAPI(lifespan=self.lifespan)
        
        # Add static files route
        self.app.mount("/static", StaticFiles(directory=self.static_dir), name="static")
        
        # Set up templates
        self.templates = Jinja2Templates(directory=self.templates_dir)
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Register routes
        self.register_routes()
        
        return self.app
    
    # ---------------------------------------
    def register_routes(self):
        """Register API routes. Override in subclasses to add specific routes."""

        @self.app.get("/")
        async def index_route(request: Request):
            # If password is required and doesn't match, show login page
            if 'nopass' not in self.args:
                password = request.cookies.get('password')
                if self._password != "" and self._password != password:
                    return self.templates.TemplateResponse('login.html', {"request": request})
            
            # If no password required or password matches, show dashboard
            return self.templates.TemplateResponse('dashboard.html', {"request": request})

        @self.app.get("/login/{password}")
        async def login_route(request: Request, password: str):
            if self._password == password:
                response = Response(content="yes")
                response.set_cookie(key="password", value=password)
                return response
            return Response(content="no")
            
        @self.app.get("/dashboard")
        async def dashboard_route(request: Request):
            # Your dashboard implementation
            return self.templates.TemplateResponse('dashboard.html', {"request": request})

        @self.app.get("/accounts")
        async def accounts_route():
            """Get all IB account codes"""
            try:
                # Get account values from IB
                accounts = list(self.ezib.accounts.keys())
                return {"accounts": accounts}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error getting accounts info: {e}")

        @self.app.get("/account/{account_id}")
        @self.app.get("/account")
        def account_route(account_id = None):
            """Get detailed info for specific account"""
            try:
                if account_id is None:
                    # Default to first account if none specified
                    account_id = next(iter(self.ezib.accounts.keys()), None)
                    if account_id is None:
                        raise HTTPException(status_code=404, detail="No accounts found")
                
                # Get account details from IB
                account_data = self.ezib.accounts.get(account_id)
                if account_data is None:
                    raise HTTPException(status_code=404, detail="Account not found")
                    
                # Format data for frontend template
                return {
                    "dailyPnL": float(account_data.get("NetLiquidation", 0)) - float(account_data.get("PreviousDayEquityWithLoanValue", 0)),
                    "unrealizedPnL": float(account_data.get("UnrealizedPnL", 0)),
                    "realizedPnL": float(account_data.get("RealizedPnL", 0)),
                    "netLiquidity": float(account_data.get("NetLiquidation", 0)),
                    "excessLiquidity": float(account_data.get("ExcessLiquidity", 0)),
                    "maintMargin": float(account_data.get("MaintMarginReq", 0)),
                    "sma": float(account_data.get("SMA", 0))
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error getting account details: {e}")
    
    # ---------------------------------------
    def load_cli_args(self):
        """
        Parse command line arguments and return only the non-default ones.
        
        Returns:
            dict: A dict of any non-default args passed on the command-line.
        """
        parser = argparse.ArgumentParser(
            description='Quant Async Reports',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
            
        parser.add_argument('--ibhost', default=self.args['ibhost'],
                          help='IB TWS/GW Server hostname', required=False)
        parser.add_argument('--ibport', default=self.args['ibport'],
                          help='TWS/GW Port to use', required=False)
        parser.add_argument('--ibclient', default=self.args['ibclient'],
                          help='TWS/GW Client ID', required=False)
        parser.add_argument('--nopass',
                            help='Skip password for web app (flag)',
                            action='store_true')

        # only return non-default cmd line args
        # (meaning only those actually given)
        cmd_args, _ = parser.parse_known_args()
        args = {k: v for k, v in vars(cmd_args).items() if v != parser.get_default(k)}
        return args
    
    # ---------------------------------------
    @property
    def lifespan(self):
        """
        Create a lifespan context manager for FastAPI.
        
        Returns:
            asynccontextmanager: A context manager for FastAPI lifespan.
        """
        @asynccontextmanager
        async def _lifespan(app: FastAPI):
            # Startup: Connect to IB when FastAPI starts
            try:
                self._logger.info("Connecting to Interactive Brokers...")
                while not self.ezib.isConnected:
                    await self.ezib.connectAsync(
                        ibhost=self.args['ibhost'], ibport=self.args['ibport'], ibclient=self.args['ibclient'])

                    await asyncio.sleep(2)

                    if not self.ezib.isConnected:
                        print('*', end="", flush=True)

                self._logger.info(f"Connected to IB at {self.ibhost}:{self.ibport}")
            except Exception as e:
                self._logger.error(f"Error connecting to IB: {e}")
            
            yield
            
            # Shutdown: Disconnect from IB when FastAPI shuts down
            try:
                await self.ezib.disconnect()
                self._logger.info("Disconnected from IB")
            except Exception as e:
                self._logger.error(f"Error disconnecting from IB: {e}")
        
        return _lifespan

    # ---------------------------------------
    def run(self, host="0.0.0.0", port=8000, reload=False, reload_dirs=["src"]):
        """
        Run the FastAPI application.
        
        Args:
            host (str): Host to run the server on
            port (int): Port to run the server on
        """
        import uvicorn
        
        # Setup the app if it hasn't been set up yet
        if self.app is None:
            self.setup_app()

        # let user know what the temp password is
        if 'nopass' not in self.args and self._password != "":
            print(" * Web app password is:", self._password)
            
        # Run the app
        uvicorn.run(self.app, host=host, port=port, reload=reload, reload_dirs=reload_dirs)