import asyncio
import argparse
import hashlib
import datetime
import logging
import asyncpg

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from contextlib import asynccontextmanager

from quant_async import tools
from quant_async.blotter import (
    Blotter, load_blotter_args
)

# Initialize the Interactive Brokers client
from ezib_async import ezIBAsync


class Reports:
    
    def __init__(self, blotter=None, host='127.0.0.1', port=5002,
                ibhost='127.0.0.1', ibport=4001, ibclient=10, 
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

        self.pool = None

        self.host = self.args['host'] if self.args['host'] is not None else host
        self.port = self.args['port'] if self.args['port'] is not None else port

        # blotter / db connection
        self.blotter_name = self.args['blotter'] if self.args['blotter'] is not None else blotter
        self.blotter_args = load_blotter_args(self.blotter_name)
        self.blotter = Blotter(**self.blotter_args)
        
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

        @self.app.get("/algos")
        async def algos():
            """
            Get all unique algos
            """
            try:
                async with self.pool.acquire() as conn:
                    records = await conn.fetch("SELECT DISTINCT algo FROM trades")
                    return records
            except asyncpg.PostgresError as e:
                # 在实际应用中应记录日志
                self._logger.error(f"Database error: {e}")
                return []



        @self.app.get("/accounts")
        async def accounts_route():
            """Get all IB account codes"""
            try:
                # Get account values from IB
                accounts = list(self.ezib.accounts.keys())
                return {"accounts": accounts}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error getting accounts info: {e}")

        @self.app.get("/trades/{start}/{end}")
        @self.app.get("/trades/{start}")
        @self.app.get("/trades")
        async def trades_route(start=None, end=None, algo_id=None, json=True):
            # 清理输入参数
            if algo_id is not None:
                algo_id = algo_id.replace('/', '')
            
            # 设置默认开始时间（7天前）
            if start is None:
                start = tools.backdate("7D", date=None, as_datetime=True)
            
            # 构建基础查询
            base_query = """
                SELECT * 
                FROM trades 
                WHERE exit_time IS NOT NULL
            """
            conditions = []
            params = []
            param_count = 1
            
            # 添加时间条件
            if start is not None:
                conditions.append(f"entry_time >= ${param_count}")
                params.append(start)
                param_count += 1
            
            if end is not None:
                conditions.append(f"exit_time <= ${param_count}")
                params.append(end)
                param_count += 1
            
            # 添加算法ID条件
            if algo_id is not None:
                conditions.append(f"algo = ${param_count}")
                params.append(algo_id)
                param_count += 1
            
            # 组合完整查询
            if conditions:
                base_query += " AND " + " AND ".join(conditions)
            
            # 添加排序
            base_query += " ORDER BY exit_time DESC, entry_time DESC"
            
            try:
                async with self.pool.acquire() as conn:
                    # 执行查询
                    records = await conn.fetch(base_query, *params)
                    
                    # 处理每条交易记录
                    processed_trades = []
                    for record in records:
                        trade = dict(record)
                        
                        # 计算滑点
                        slippage = abs(trade['entry_price'] - trade['market_price'])
                        
                        # 根据方向调整滑点正负
                        if ((trade['direction'] == "LONG" and trade['entry_price'] > trade['market_price']) or
                            (trade['direction'] == "SHORT" and trade['entry_price'] < trade['market_price'])):
                            slippage = -slippage
                        
                        trade['slippage'] = slippage
                        processed_trades.append(trade)
                    
                    return processed_trades
            
            except asyncpg.PostgresError as e:
                self._logger.error(f"Database error in trades query: {str(e)}")
                return []

        @self.app.get("/positions/{algo_id}")
        @self.app.get("/positions")
        async def positions_route(algo_id=None, json=True):
            """
            Get all IB positions
            """
            if algo_id is not None:
                algo_id = algo_id.replace('/', '')

            trades_query = "SELECT * FROM trades WHERE exit_time IS NULL"
            
            params = []
            
            if algo_id is not None:
                trades_query += " AND algo='" + algo_id + "'"

            # 获取最新价格查询
            last_price_query = """
                SELECT s.id AS symbol_id, MAX(t.last) AS last_price
                FROM ticks t
                JOIN symbols s ON t.symbol_id = s.id
                GROUP BY s.id
            """

            try:
                processed_trades = []
                async with self.pool.acquire() as conn:
                    # 执行交易查询
                    trades_records = await conn.fetch(trades_query, *params)
                    
                    # 执行最新价格查询
                    last_prices = await conn.fetch(last_price_query)
                    
                    # 将最新价格转换为字典 {symbol_id: last_price}
                    price_map = {record['symbol_id']: record['last_price'] for record in last_prices}
                    
                    # 处理交易记录
                    for trade in trades_records:
                        # 转换为字典
                        trade_dict = dict(trade)
                        
                        # 获取该交易的最新价格
                        last_price = price_map.get(trade_dict['symbol'])
                        
                        # 计算未实现盈亏
                        if last_price is not None:
                            if trade_dict['direction'] == "SHORT":
                                unrealized_pnl = trade_dict['entry_price'] - last_price
                            else:  # LONG
                                unrealized_pnl = last_price - trade_dict['entry_price']
                        else:
                            unrealized_pnl = 0.0
                        
                        # 计算滑点
                        slippage = abs(trade_dict['entry_price'] - trade_dict['market_price'])
                        if ((trade_dict['direction'] == "LONG" and trade_dict['entry_price'] > trade_dict['market_price']) or
                            (trade_dict['direction'] == "SHORT" and trade_dict['entry_price'] < trade_dict['market_price'])):
                            slippage = -slippage
                        
                        # 添加计算字段
                        trade_dict['last_price'] = last_price or 0.0
                        trade_dict['unrealized_pnl'] = unrealized_pnl
                        trade_dict['slippage'] = slippage
                        
                        processed_trades.append(trade_dict)
                    
                    # 按入场时间降序排序
                    processed_trades.sort(key=lambda x: x['entry_time'], reverse=True)
            
            except asyncpg.PostgresError as e:
                # 在实际应用中应记录日志
                self._logger.error(f"Database error: {e}")
                processed_trades = []

            return processed_trades
            
        
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
                # self._logger.info(account_data)
                if account_data is None:
                    raise HTTPException(status_code=404, detail="Account not found")
                
                # account_value = {item.tag: item.value for item in account_data}
                    
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
                self._logger.info(f"Connecting to Interactive Brokers at: {self.args['ibport']} (client: {self.args['ibclient']})")
                while not self.ezib.connected:
                    await self.ezib.connectAsync(
                        ibhost=self.args['ibhost'], ibport=self.args['ibport'], ibclient=self.args['ibclient'])

                    await asyncio.sleep(2)

                    if not self.ezib.connected:
                        print('*', end="", flush=True)

                self._logger.info(f"Connected to IB at {self.ibhost}:{self.ibport} (clientId: {self.ibclient})")

                # connect to postgres using blotter's settings
                self.pool = await asyncpg.create_pool(
                    host=str(self.blotter_args['dbhost']),
                    port=int(self.blotter_args['dbport']),
                    user=str(self.blotter_args['dbuser']),
                    password=str(self.blotter_args['dbpass']),
                    database=str(self.blotter_args['dbname']),
                    min_size=5,
                    max_size=20
                )
                if self.pool is None:
                    raise HTTPException(status_code=500, detail="Database connection pool not initialized")
            except Exception as e:
                self._logger.error(f"Error connecting to IB: {e}")
            
            yield
            
            # Shutdown: Disconnect from IB when FastAPI shuts down
            try:
                self.ezib.disconnect()
                self._logger.info("Dconnected from IB")

                # close postgres connection pool
                if self.pool:
                    await self.pool.close()
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