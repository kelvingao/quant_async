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

import logging
import asyncpg
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal


class DataStore:
    """
    High-performance data storage engine for market data.
    
    Handles batch operations, data validation, and optimized database operations
    for financial time-series data storage and retrieval.
    """
    
    def __init__(self, pool: asyncpg.Pool):
        """
        Initialize DataStore with asyncpg connection pool.
        
        Args:
            pool (asyncpg.Pool): PostgreSQL connection pool
        """
        self.pool = pool
        self._logger = logging.getLogger('quant_async.storage')
        
    # ---------------------------------------
    # Symbol Management
    # ---------------------------------------
    
    async def get_symbol_id(self, symbol: str, asset_class: str = 'STK', 
                           exchange: str = 'SMART', currency: str = 'USD',
                           expiry: Optional[datetime] = None) -> int:
        """
        Get or create symbol ID for database operations.
        
        Args:
            symbol (str): Symbol name (e.g., 'AAPL')
            asset_class (str, optional): Asset class. Defaults to 'STK'.
            exchange (str, optional): Exchange. Defaults to 'SMART'.
            currency (str, optional): Currency. Defaults to 'USD'.
            expiry (datetime, optional): Expiry date for derivatives. Defaults to None.
            
        Returns:
            int: Symbol ID from database
            
        Raises:
            asyncpg.PostgresError: If database operation fails
        """
        async with self.pool.acquire() as conn:
            # Try to find existing symbol
            query_select = """
                SELECT id FROM symbols 
                WHERE symbol = $1 AND asset_class = $2
            """
            
            symbol_id = await conn.fetchval(query_select, symbol, asset_class)
            
            if symbol_id:
                return symbol_id
            
            # Create new symbol entry
            query_insert = """
                INSERT INTO symbols (symbol, symbol_group, asset_class, expiry)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """
            
            symbol_group = symbol  # Use symbol as group for simplicity
            
            try:
                symbol_id = await conn.fetchval(
                    query_insert, symbol, symbol_group, asset_class, expiry
                )
                self._logger.info(f"Created new symbol entry: {symbol} (ID: {symbol_id})")
                return symbol_id
                
            except asyncpg.UniqueViolationError:
                # Race condition - symbol was created by another process
                symbol_id = await conn.fetchval(query_select, symbol, asset_class)
                if symbol_id:
                    return symbol_id
                raise
    
    # ---------------------------------------
    # Batch Operations for High Performance
    # ---------------------------------------
    
    async def save_bars_batch(self, bars: List[Dict[str, Any]]) -> int:
        """
        Batch insert OHLCV bars with optimized performance.
        
        Args:
            bars (List[Dict]): List of bar data dictionaries with keys:
                - symbol: str
                - datetime: datetime
                - open, high, low, close: float
                - volume: int
                - asset_class: str (optional, defaults to 'STK')
                
        Returns:
            int: Number of bars successfully inserted
            
        Raises:
            ValueError: If bars data is invalid
            asyncpg.PostgresError: If database operation fails
        """
        if not bars:
            return 0
            
        self._logger.info(f"Saving {len(bars)} bars to database")
        
        try:
            # Use COPY for large batches (>1000 records)
            if len(bars) > 1000:
                return await self._copy_insert_bars(bars)
            else:
                return await self._batch_insert_bars(bars)
                
        except Exception as e:
            error_msg = f"Failed to save bars batch: {e}"
            self._logger.error(error_msg)
            raise
    
    async def _copy_insert_bars(self, bars: List[Dict[str, Any]]) -> int:
        """
        Use PostgreSQL COPY for high-performance bulk insert.
        
        Args:
            bars (List[Dict]): Bar data
            
        Returns:
            int: Number of bars inserted
        """
        # Prepare data for COPY
        records = []
        symbol_cache = {}  # Cache symbol IDs
        
        async with self.pool.acquire() as conn:
            for bar in bars:
                # Get symbol ID (with caching)
                symbol = bar['symbol']
                asset_class = bar.get('asset_class', 'STK')
                cache_key = f"{symbol}_{asset_class}"
                
                if cache_key not in symbol_cache:
                    symbol_cache[cache_key] = await self.get_symbol_id(
                        symbol, asset_class
                    )
                
                symbol_id = symbol_cache[cache_key]
                
                # Prepare record tuple
                record = (
                    bar['datetime'],
                    symbol_id,
                    Decimal(str(bar['open'])),
                    Decimal(str(bar['high'])),
                    Decimal(str(bar['low'])),
                    Decimal(str(bar['close'])),
                    int(bar['volume'])
                )
                records.append(record)
            
            # Use COPY for bulk insert
            columns = ['datetime', 'symbol_id', 'open', 'high', 'low', 'close', 'volume']
            
            try:
                await conn.copy_records_to_table(
                    'bars', 
                    records=records, 
                    columns=columns
                )
                
                self._logger.info(f"COPY inserted {len(records)} bars successfully")
                return len(records)
                
            except asyncpg.UniqueViolationError as e:
                self._logger.warning(f"Duplicate bars detected in COPY operation: {e}")
                # Fall back to individual inserts with ON CONFLICT handling
                return await self._batch_insert_bars_with_conflicts(bars)
    
    async def _batch_insert_bars(self, bars: List[Dict[str, Any]]) -> int:
        """
        Standard batch insert for smaller datasets.
        
        Args:
            bars (List[Dict]): Bar data
            
        Returns:
            int: Number of bars inserted
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                query = """
                    INSERT INTO bars (datetime, symbol_id, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (datetime, symbol_id) DO NOTHING
                """
                
                inserted_count = 0
                symbol_cache = {}
                
                for bar in bars:
                    # Get symbol ID (with caching)
                    symbol = bar['symbol']
                    asset_class = bar.get('asset_class', 'STK')
                    cache_key = f"{symbol}_{asset_class}"
                    
                    if cache_key not in symbol_cache:
                        symbol_cache[cache_key] = await self.get_symbol_id(
                            symbol, asset_class
                        )
                    
                    symbol_id = symbol_cache[cache_key]
                    
                    try:
                        await conn.execute(
                            query,
                            bar['datetime'],
                            symbol_id,
                            Decimal(str(bar['open'])),
                            Decimal(str(bar['high'])),
                            Decimal(str(bar['low'])),
                            Decimal(str(bar['close'])),
                            int(bar['volume'])
                        )
                        inserted_count += 1
                        
                    except Exception as e:
                        self._logger.warning(f"Failed to insert bar for {symbol}: {e}")
                
                self._logger.info(f"Batch inserted {inserted_count}/{len(bars)} bars")
                return inserted_count
    
    async def _batch_insert_bars_with_conflicts(self, bars: List[Dict[str, Any]]) -> int:
        """
        Handle batch insert with conflict resolution.
        
        Args:
            bars (List[Dict]): Bar data
            
        Returns:
            int: Number of bars inserted
        """
        return await self._batch_insert_bars(bars)
    
    async def save_ticks_batch(self, ticks: List[Dict[str, Any]]) -> int:
        """
        Batch insert tick data with optimized performance.
        
        Args:
            ticks (List[Dict]): List of tick data dictionaries with keys:
                - symbol: str
                - datetime: datetime
                - bid, ask, last: float
                - bidsize, asksize, lastsize: int
                - asset_class: str (optional, defaults to 'STK')
                
        Returns:
            int: Number of ticks successfully inserted
        """
        if not ticks:
            return 0
            
        self._logger.info(f"Saving {len(ticks)} ticks to database")
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                query = """
                    INSERT INTO ticks (datetime, symbol_id, bid, bidsize, ask, asksize, last, lastsize)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (datetime, symbol_id) DO NOTHING
                """
                
                inserted_count = 0
                symbol_cache = {}
                
                for tick in ticks:
                    # Get symbol ID (with caching)
                    symbol = tick['symbol']
                    asset_class = tick.get('asset_class', 'STK')
                    cache_key = f"{symbol}_{asset_class}"
                    
                    if cache_key not in symbol_cache:
                        symbol_cache[cache_key] = await self.get_symbol_id(
                            symbol, asset_class
                        )
                    
                    symbol_id = symbol_cache[cache_key]
                    
                    try:
                        await conn.execute(
                            query,
                            tick['datetime'],
                            symbol_id,
                            Decimal(str(tick.get('bid', 0))) if tick.get('bid') else None,
                            int(tick.get('bidsize', 0)) if tick.get('bidsize') else None,
                            Decimal(str(tick.get('ask', 0))) if tick.get('ask') else None,
                            int(tick.get('asksize', 0)) if tick.get('asksize') else None,
                            Decimal(str(tick.get('last', 0))) if tick.get('last') else None,
                            int(tick.get('lastsize', 0)) if tick.get('lastsize') else None
                        )
                        inserted_count += 1
                        
                    except Exception as e:
                        self._logger.warning(f"Failed to insert tick for {symbol}: {e}")
                
                self._logger.info(f"Batch inserted {inserted_count}/{len(ticks)} ticks")
                return inserted_count
    
    # ---------------------------------------
    # Historical Data Retrieval
    # ---------------------------------------
    
    async def get_historical_bars(self, symbols: List[str], start: datetime, 
                                 end: datetime, asset_class: str = 'STK') -> pd.DataFrame:
        """
        Retrieve historical bar data for multiple symbols.
        
        Args:
            symbols (List[str]): List of symbols to query
            start (datetime): Start date for data retrieval
            end (datetime): End date for data retrieval
            asset_class (str, optional): Asset class filter. Defaults to 'STK'.
            
        Returns:
            pandas.DataFrame: Historical bar data with MultiIndex (symbol, datetime)
        """
        async with self.pool.acquire() as conn:
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
                    AND s.asset_class = $2
                    AND b.datetime BETWEEN $3 AND $4
                ORDER BY s.symbol, b.datetime
            """
            
            rows = await conn.fetch(query, symbols, asset_class, start, end)
            
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame([dict(row) for row in rows])
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df.set_index(['symbol', 'datetime'], inplace=True)
            
            return df
    
    async def get_historical_ticks(self, symbols: List[str], start: datetime,
                                  end: datetime, asset_class: str = 'STK') -> pd.DataFrame:
        """
        Retrieve historical tick data for multiple symbols.
        
        Args:
            symbols (List[str]): List of symbols to query
            start (datetime): Start date for data retrieval
            end (datetime): End date for data retrieval
            asset_class (str, optional): Asset class filter. Defaults to 'STK'.
            
        Returns:
            pandas.DataFrame: Historical tick data with MultiIndex (symbol, datetime)
        """
        async with self.pool.acquire() as conn:
            query = """
                SELECT 
                    s.symbol,
                    t.datetime,
                    t.bid,
                    t.bidsize,
                    t.ask,
                    t.asksize,
                    t.last,
                    t.lastsize
                FROM ticks t
                JOIN symbols s ON t.symbol_id = s.id
                WHERE s.symbol = ANY($1)
                    AND s.asset_class = $2
                    AND t.datetime BETWEEN $3 AND $4
                ORDER BY s.symbol, t.datetime
            """
            
            rows = await conn.fetch(query, symbols, asset_class, start, end)
            
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame([dict(row) for row in rows])
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
                df.set_index(['symbol', 'datetime'], inplace=True)
            
            return df
    
    # ---------------------------------------
    # Data Validation and Cleaning
    # ---------------------------------------
    
    def validate_bar_data(self, bar: Dict[str, Any]) -> bool:
        """
        Validate bar data before insertion.
        
        Args:
            bar (Dict): Bar data dictionary
            
        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume']
        
        # Check required fields
        for field in required_fields:
            if field not in bar or bar[field] is None:
                self._logger.warning(f"Missing required field '{field}' in bar data")
                return False
        
        # Validate OHLC logic
        try:
            open_price, high_price, low_price, close_price = float(bar['open']), float(bar['high']), float(bar['low']), float(bar['close'])
            
            if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
                self._logger.warning(f"Invalid OHLC data: O={open_price}, H={high_price}, L={low_price}, C={close_price}")
                return False
            
            if bar['volume'] < 0:
                self._logger.warning(f"Invalid volume: {bar['volume']}")
                return False
                
        except (ValueError, TypeError) as e:
            self._logger.warning(f"Invalid numeric data in bar: {e}")
            return False
        
        return True
    
    def validate_tick_data(self, tick: Dict[str, Any]) -> bool:
        """
        Validate tick data before insertion.
        
        Args:
            tick (Dict): Tick data dictionary
            
        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = ['symbol', 'datetime']
        
        # Check required fields
        for field in required_fields:
            if field not in tick or tick[field] is None:
                self._logger.warning(f"Missing required field '{field}' in tick data")
                return False
        
        # Validate bid/ask logic
        try:
            bid = tick.get('bid')
            ask = tick.get('ask')
            
            if bid is not None and ask is not None:
                if float(bid) > float(ask):
                    self._logger.warning(f"Invalid bid/ask spread: bid={bid}, ask={ask}")
                    return False
                    
        except (ValueError, TypeError) as e:
            self._logger.warning(f"Invalid numeric data in tick: {e}")
            return False
        
        return True