import asyncio
import argparse
import hashlib
import datetime
import logging
import asyncpg
import pandas as pd
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Query
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

        # ===========================================
        # NEW DATA API ENDPOINTS
        # ===========================================

        @self.app.get("/api/v1/history/{symbol}")
        async def get_history(
            symbol: str,
            start: str = Query(..., description="Start date in ISO format (e.g., '2024-01-01' or '2024-01-01T10:00:00')"),
            end: Optional[str] = Query(None, description="End date in ISO format (defaults to now)"),
            resolution: str = Query("1T", description="Time resolution ('1T', '5T', '1H', '1D')")
        ):
            """
            Retrieve historical market data for a symbol.
            
            Args:
                symbol: Symbol to query (e.g., 'AAPL', 'EURUSD')
                start: Start datetime in ISO format
                end: End datetime in ISO format (optional, defaults to now)
                resolution: Time resolution for data aggregation
                
            Returns:
                JSON response with historical data
            """
            try:
                self._logger.info(f"History API request: symbol={symbol}, start={start}, end={end}, resolution={resolution}")
                
                # Parse datetime strings
                try:
                    start_dt = pd.to_datetime(start)
                    end_dt = pd.to_datetime(end) if end else datetime.datetime.now()
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
                
                # Validate symbol format and resolution
                if not symbol or len(symbol.strip()) == 0:
                    raise HTTPException(status_code=400, detail="Symbol cannot be empty")
                
                if resolution not in ['1T', '5T', '15T', '30T', '1H', '4H', '1D']:
                    raise HTTPException(status_code=400, detail=f"Unsupported resolution: {resolution}")
                
                # Use the enhanced blotter.history() method
                try:
                    df = await self.blotter.history(
                        symbols=[symbol.upper()],
                        start=start_dt,
                        end=end_dt,
                        resolution=resolution,
                        tz='UTC'
                    )
                    
                    if df.empty:
                        return {
                            "status": "success",
                            "symbol": symbol.upper(),
                            "data": [],
                            "count": 0,
                            "message": "No data found for the specified time range"
                        }
                    
                    # Convert DataFrame to records format for JSON response
                    # Reset index to include datetime as a column
                    df_reset = df.reset_index()
                    data_records = df_reset.to_dict("records")
                    
                    # Convert datetime objects to ISO strings for JSON serialization
                    for record in data_records:
                        if 'datetime' in record and pd.notna(record['datetime']):
                            record['datetime'] = record['datetime'].isoformat()
                    
                    return {
                        "status": "success",
                        "symbol": symbol.upper(),
                        "start": start_dt.isoformat(),
                        "end": end_dt.isoformat(),
                        "resolution": resolution,
                        "data": data_records,
                        "count": len(data_records)
                    }
                    
                except Exception as e:
                    error_msg = f"Database query failed: {str(e)}"
                    self._logger.error(error_msg)
                    raise HTTPException(status_code=500, detail=error_msg)
                    
            except HTTPException:
                raise
            except Exception as e:
                error_msg = f"Unexpected error in history endpoint: {str(e)}"
                self._logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)

        @self.app.get("/api/v1/realtime/{symbol}")
        async def get_realtime(symbol: str):
            """
            Get the latest real-time market data for a symbol.
            
            Args:
                symbol: Symbol to query
                
            Returns:
                JSON response with latest market data
            """
            try:
                self._logger.info(f"Realtime API request: symbol={symbol}")
                
                if not symbol or len(symbol.strip()) == 0:
                    raise HTTPException(status_code=400, detail="Symbol cannot be empty")
                
                symbol = symbol.upper()
                
                # Get the latest tick data from database
                async with self.pool.acquire() as conn:
                    # Query latest tick data
                    tick_query = """
                        SELECT 
                            t.datetime,
                            t.bid,
                            t.bidsize,
                            t.ask,
                            t.asksize,
                            t.last,
                            t.lastsize,
                            s.symbol
                        FROM ticks t
                        JOIN symbols s ON t.symbol_id = s.id
                        WHERE s.symbol = $1
                        ORDER BY t.datetime DESC
                        LIMIT 1
                    """
                    
                    # Query latest bar data
                    bar_query = """
                        SELECT 
                            b.datetime,
                            b.open,
                            b.high,
                            b.low,
                            b.close,
                            b.volume,
                            s.symbol
                        FROM bars b
                        JOIN symbols s ON b.symbol_id = s.id
                        WHERE s.symbol = $1
                        ORDER BY b.datetime DESC
                        LIMIT 1
                    """
                    
                    # Execute both queries
                    tick_row = await conn.fetchrow(tick_query, symbol)
                    bar_row = await conn.fetchrow(bar_query, symbol)
                    
                    # Prepare response data
                    response_data = {
                        "symbol": symbol,
                        "timestamp": datetime.datetime.now().isoformat(),
                        "tick_data": None,
                        "bar_data": None
                    }
                    
                    if tick_row:
                        response_data["tick_data"] = {
                            "datetime": tick_row['datetime'].isoformat() if tick_row['datetime'] else None,
                            "bid": float(tick_row['bid']) if tick_row['bid'] else None,
                            "bidsize": tick_row['bidsize'],
                            "ask": float(tick_row['ask']) if tick_row['ask'] else None,
                            "asksize": tick_row['asksize'],
                            "last": float(tick_row['last']) if tick_row['last'] else None,
                            "lastsize": tick_row['lastsize']
                        }
                    
                    if bar_row:
                        response_data["bar_data"] = {
                            "datetime": bar_row['datetime'].isoformat() if bar_row['datetime'] else None,
                            "open": float(bar_row['open']) if bar_row['open'] else None,
                            "high": float(bar_row['high']) if bar_row['high'] else None,
                            "low": float(bar_row['low']) if bar_row['low'] else None,
                            "close": float(bar_row['close']) if bar_row['close'] else None,
                            "volume": bar_row['volume']
                        }
                    
                    if not tick_row and not bar_row:
                        return {
                            "status": "success",
                            "symbol": symbol,
                            "data": response_data,
                            "message": "No recent data available for this symbol"
                        }
                    
                    return {
                        "status": "success",
                        "symbol": symbol,
                        "data": response_data
                    }
                    
            except HTTPException:
                raise
            except Exception as e:
                error_msg = f"Error retrieving realtime data: {str(e)}"
                self._logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
        
        # =======================================
        # Enhanced Data Retrieval Endpoints
        # =======================================
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            return {
                "status": "healthy",
                "timestamp": datetime.datetime.now().isoformat(),
                "database": "connected" if hasattr(self, 'pool') and self.pool else "disconnected"
            }
        
        @self.app.get("/symbols")
        async def get_symbols():
            """Get all available symbols"""
            try:
                if not hasattr(self, 'pool') or not self.pool:
                    raise HTTPException(status_code=503, detail="Database not available")
                
                async with self.pool.acquire() as conn:
                    symbols = await conn.fetch("""
                        SELECT id, symbol, symbol_group, asset_class, expiry
                        FROM symbols 
                        ORDER BY symbol
                    """)
                    
                return {
                    "symbols": [dict(s) for s in symbols],
                    "count": len(symbols)
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error retrieving symbols: {str(e)}")
        
        @self.app.get("/symbols/{symbol}")
        async def get_symbol_details(symbol: str):
            """Get detailed information for a specific symbol"""
            try:
                if not hasattr(self, 'pool') or not self.pool:
                    raise HTTPException(status_code=503, detail="Database not available")
                
                async with self.pool.acquire() as conn:
                    # Check if symbol exists
                    symbol_info = await conn.fetchrow("""
                        SELECT id, symbol, symbol_group, asset_class
                        FROM symbols 
                        WHERE symbol = $1
                    """, symbol)
                    
                    if not symbol_info:
                        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
                    
                    # Get bars statistics
                    bars_stats = await conn.fetchrow("""
                        SELECT 
                            COUNT(*) as count,
                            MIN(open) as min_price,
                            MAX(high) as max_price,
                            MIN(volume) as min_volume,
                            MAX(volume) as max_volume,
                            MIN(datetime) as earliest,
                            MAX(datetime) as latest
                        FROM bars 
                        WHERE symbol_id = $1
                    """, symbol_info['id'])
                    
                    # Get ticks statistics  
                    ticks_stats = await conn.fetchrow("""
                        SELECT 
                            COUNT(*) as count,
                            MIN(datetime) as earliest,
                            MAX(datetime) as latest
                        FROM ticks 
                        WHERE symbol_id = $1
                    """, symbol_info['id'])
                    
                return {
                    "symbol": symbol_info['symbol'],
                    "asset_class": symbol_info['asset_class'],
                    "symbol_group": symbol_info['symbol_group'],
                    "bars_count": bars_stats['count'] if bars_stats else 0,
                    "ticks_count": ticks_stats['count'] if ticks_stats else 0,
                    "price_range": {
                        "min": float(bars_stats['min_price']) if bars_stats and bars_stats['min_price'] else None,
                        "max": float(bars_stats['max_price']) if bars_stats and bars_stats['max_price'] else None
                    },
                    "volume_range": {
                        "min": bars_stats['min_volume'] if bars_stats and bars_stats['min_volume'] else None,
                        "max": bars_stats['max_volume'] if bars_stats and bars_stats['max_volume'] else None
                    },
                    "data_range": {
                        "earliest": bars_stats['earliest'].isoformat() if bars_stats and bars_stats['earliest'] else None,
                        "latest": bars_stats['latest'].isoformat() if bars_stats and bars_stats['latest'] else None
                    }
                }
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error retrieving symbol details: {str(e)}")
        
        @self.app.get("/historical/bars")
        async def get_historical_bars(
            symbols: str = Query(..., description="Comma-separated list of symbols"),
            start: str = Query(..., description="Start date (YYYY-MM-DD)"),
            end: str = Query(..., description="End date (YYYY-MM-DD)")
        ):
            """Get historical bar data"""
            try:
                if not hasattr(self, 'pool') or not self.pool:
                    raise HTTPException(status_code=503, detail="Database not available")
                
                # Parse dates
                try:
                    start_dt = pd.to_datetime(start).to_pydatetime()
                    end_dt = pd.to_datetime(end).to_pydatetime()
                    if start_dt >= end_dt:
                        raise HTTPException(status_code=400, detail="Start date must be before end date")
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
                
                symbols_list = [s.strip() for s in symbols.split(',')]
                
                async with self.pool.acquire() as conn:
                    bars = await conn.fetch("""
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
                    """, symbols_list, start_dt, end_dt)
                    
                return {
                    "data": [dict(bar) for bar in bars],
                    "count": len(bars),
                    "symbols": symbols_list,
                    "date_range": {
                        "start": start,
                        "end": end
                    }
                }
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error retrieving historical bars: {str(e)}")
        
        @self.app.get("/historical/ticks")  
        async def get_historical_ticks(
            symbols: str = Query(..., description="Comma-separated list of symbols"),
            start: str = Query(..., description="Start date (YYYY-MM-DD)"),
            end: str = Query(..., description="End date (YYYY-MM-DD)")
        ):
            """Get historical tick data"""
            try:
                if not hasattr(self, 'pool') or not self.pool:
                    raise HTTPException(status_code=503, detail="Database not available")
                
                # Parse dates
                try:
                    start_dt = pd.to_datetime(start).to_pydatetime()
                    end_dt = pd.to_datetime(end).to_pydatetime()
                    if start_dt >= end_dt:
                        raise HTTPException(status_code=400, detail="Start date must be before end date")
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
                
                symbols_list = [s.strip() for s in symbols.split(',')]
                
                async with self.pool.acquire() as conn:
                    ticks = await conn.fetch("""
                        SELECT 
                            s.symbol,
                            t.datetime,
                            t.bid,
                            t.ask,
                            t.last,
                            t.bidsize,
                            t.asksize,
                            t.lastsize
                        FROM ticks t
                        JOIN symbols s ON t.symbol_id = s.id
                        WHERE s.symbol = ANY($1)
                        AND t.datetime BETWEEN $2 AND $3
                        ORDER BY s.symbol, t.datetime
                    """, symbols_list, start_dt, end_dt)
                    
                return {
                    "data": [dict(tick) for tick in ticks],
                    "count": len(ticks),
                    "symbols": symbols_list,
                    "date_range": {
                        "start": start,
                        "end": end
                    }
                }
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error retrieving historical ticks: {str(e)}")
        
        @self.app.get("/statistics/summary")
        async def get_statistics_summary():
            """Get overall database statistics"""
            try:
                if not hasattr(self, 'pool') or not self.pool:
                    raise HTTPException(status_code=503, detail="Database not available")
                
                async with self.pool.acquire() as conn:
                    # Get overall statistics
                    stats = await conn.fetchrow("""
                        SELECT 
                            (SELECT COUNT(*) FROM symbols) as total_symbols,
                            (SELECT COUNT(*) FROM bars) as total_bars,
                            (SELECT COUNT(*) FROM ticks) as total_ticks,
                            (SELECT MIN(datetime) FROM bars) as earliest_bar,
                            (SELECT MAX(datetime) FROM bars) as latest_bar
                    """)
                    
                return {
                    "total_symbols": stats['total_symbols'],
                    "total_bars": stats['total_bars'],
                    "total_ticks": stats['total_ticks'],
                    "data_range": {
                        "start": stats['earliest_bar'].isoformat() if stats['earliest_bar'] else None,
                        "end": stats['latest_bar'].isoformat() if stats['latest_bar'] else None
                    }
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error retrieving statistics: {str(e)}")
        
        @self.app.get("/metrics/performance")
        async def get_performance_metrics():
            """Get performance metrics"""
            try:
                # Mock performance data since we don't have real metrics collection yet
                return {
                    "metrics": ["database_queries", "api_requests", "response_times"],
                    "database": {
                        "total_queries": 156,
                        "avg_query_time": 0.023,
                        "connection_pool_size": 5
                    },
                    "api": {
                        "total_requests": 89,
                        "avg_response_time": 0.045,
                        "success_rate": 0.97
                    }
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error retrieving performance metrics: {str(e)}")
        
        @self.app.get("/quality/validation")
        async def get_data_quality():
            """Get data quality validation results"""
            try:
                if not hasattr(self, 'pool') or not self.pool:
                    raise HTTPException(status_code=503, detail="Database not available")
                
                async with self.pool.acquire() as conn:
                    # Check for data quality issues
                    invalid_bars = await conn.fetchrow("""
                        SELECT COUNT(*) as count FROM bars
                        WHERE high < open OR high < close OR low > open OR low > close OR volume < 0
                    """)
                    
                    total_bars = await conn.fetchrow("SELECT COUNT(*) as count FROM bars")
                    total_ticks = await conn.fetchrow("SELECT COUNT(*) as count FROM ticks")
                    
                return {
                    "overall_status": "good" if invalid_bars['count'] == 0 else "issues_found",
                    "bars": {
                        "total_count": total_bars['count'],
                        "valid_count": total_bars['count'] - invalid_bars['count'],
                        "invalid_count": invalid_bars['count']
                    },
                    "ticks": {
                        "total_count": total_ticks['count'],
                        "valid_count": total_ticks['count'],  # Assume all ticks are valid for now
                        "invalid_count": 0
                    },
                    "issues": [
                        {
                            "type": "invalid_ohlc",
                            "description": f"Found {invalid_bars['count']} bars with invalid OHLC relationships"
                        }
                    ] if invalid_bars['count'] > 0 else []
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error retrieving data quality: {str(e)}")
    
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