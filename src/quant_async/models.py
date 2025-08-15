from typing import Any, Dict, Optional
from decimal import Decimal
from sqlalchemy import (
    Column, Integer, BigInteger, String, Date, DateTime, Numeric,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, relationship

Base: Any = declarative_base()

class Version(Base):
    __tablename__ = '_version_'

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(8), nullable=True)

class Symbols(Base):
    __tablename__ = 'symbols'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(24), nullable=True, index=True)
    symbol_group = Column(String(18), nullable=True, index=True)
    asset_class = Column(String(3), nullable=True, index=True)
    expiry = Column(Date, nullable=True, index=True)

    # Optional: Define relationships for easier querying if needed later
    bars = relationship("Bars", back_populates="symbol_ref")
    ticks = relationship("Ticks", back_populates="symbol_ref")

class Bars(Base):
    __tablename__ = 'bars'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Use timezone=False if your datetimes don't have timezone info
    datetime = Column(DateTime(timezone=False), nullable=False, index=True)
    symbol_id = Column(Integer, ForeignKey('symbols.id'), nullable=False, index=True)
    open = Column(Numeric, nullable=True) # Use Numeric for precision
    high = Column(Numeric, nullable=True)
    low = Column(Numeric, nullable=True)
    close = Column(Numeric, nullable=True)
    volume = Column(BigInteger, nullable=True) # Use BigInteger for potentially large volumes

    symbol_ref = relationship("Symbols", back_populates="bars") # Relationship back to Symbols
    greeks = relationship("Greeks", back_populates="bar_ref") # Relationship to Greeks

    __table_args__ = (
        # Composite index for time-series queries (most common pattern)
        Index('idx_bars_symbol_datetime', 'symbol_id', 'datetime', postgresql_using='btree'),
        # BRIN index for large time-series datasets with natural ordering
        Index('idx_bars_datetime_brin', 'datetime', postgresql_using='brin'),
        # Unique constraint to prevent duplicate entries
        UniqueConstraint('datetime', 'symbol_id', name='uq_bar_datetime_symbol'),
    )

    @classmethod
    def validate_bar_data(cls, data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate OHLCV bar data for logical consistency.
        
        Args:
            data: Dictionary containing bar data
            
        Returns:
            tuple: (is_valid, error_message)
        """
        try:
            # Check required fields
            required_fields = ['open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return False, f"Missing required field: {field}"
            
            # Convert to Decimal for precise comparison
            open_price = Decimal(str(data['open']))
            high_price = Decimal(str(data['high']))
            low_price = Decimal(str(data['low']))
            close_price = Decimal(str(data['close']))
            
            # Validate OHLC logic: high >= max(open, close) and low <= min(open, close)
            if high_price < max(open_price, close_price):
                return False, f"High ({high_price}) must be >= max(open={open_price}, close={close_price})"
            
            if low_price > min(open_price, close_price):
                return False, f"Low ({low_price}) must be <= min(open={open_price}, close={close_price})"
            
            # Validate volume is non-negative
            volume = int(data['volume'])
            if volume < 0:
                return False, f"Volume ({volume}) must be non-negative"
            
            return True, None
            
        except (ValueError, TypeError, KeyError) as e:
            return False, f"Data validation error: {str(e)}"

class Ticks(Base):
    __tablename__ = 'ticks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Consider TIMESTAMP(precision=3) if needed and supported by dialect
    datetime = Column(DateTime(timezone=False), nullable=False, index=True)
    symbol_id = Column(Integer, ForeignKey('symbols.id'), nullable=False, index=True)
    bid = Column(Numeric, nullable=True)
    bidsize = Column(Integer, nullable=True)
    ask = Column(Numeric, nullable=True)
    asksize = Column(Integer, nullable=True)
    last = Column(Numeric, nullable=True)
    lastsize = Column(Integer, nullable=True)

    symbol_ref = relationship("Symbols", back_populates="ticks") # Relationship back to Symbols
    greeks = relationship("Greeks", back_populates="tick_ref") # Relationship to Greeks

    __table_args__ = (
        # Composite index for tick queries by symbol and time
        Index('idx_ticks_symbol_datetime', 'symbol_id', 'datetime', postgresql_using='btree'),
        # BRIN index for large tick datasets with time-based ordering
        Index('idx_ticks_datetime_brin', 'datetime', postgresql_using='brin'),
        # Unique constraint to prevent duplicate tick entries
        UniqueConstraint('datetime', 'symbol_id', name='uq_tick_datetime_symbol'),
    )

    @classmethod
    def validate_tick_data(cls, data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate tick data for logical consistency.
        
        Args:
            data: Dictionary containing tick data
            
        Returns:
            tuple: (is_valid, error_message)
        """
        try:
            # Check if bid/ask spread is valid
            bid = data.get('bid')
            ask = data.get('ask')
            
            if bid is not None and ask is not None:
                bid_decimal = Decimal(str(bid))
                ask_decimal = Decimal(str(ask))
                
                if bid_decimal > ask_decimal:
                    return False, f"Bid ({bid_decimal}) cannot be greater than ask ({ask_decimal})"
            
            # Validate sizes are non-negative
            for size_field in ['bidsize', 'asksize', 'lastsize']:
                if size_field in data and data[size_field] is not None:
                    size = int(data[size_field])
                    if size < 0:
                        return False, f"{size_field} ({size}) must be non-negative"
            
            # Validate prices are non-negative
            for price_field in ['bid', 'ask', 'last']:
                if price_field in data and data[price_field] is not None:
                    price = Decimal(str(data[price_field]))
                    if price < 0:
                        return False, f"{price_field} ({price}) must be non-negative"
            
            return True, None
            
        except (ValueError, TypeError) as e:
            return False, f"Tick data validation error: {str(e)}"

class Greeks(Base):
    __tablename__ = 'greeks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tick_id = Column(Integer, ForeignKey('ticks.id'), nullable=True, index=True)
    bar_id = Column(Integer, ForeignKey('bars.id'), nullable=True, index=True)
    price = Column(Numeric, nullable=True)
    underlying = Column(Numeric, nullable=True)
    dividend = Column(Numeric, nullable=True)
    volume = Column(Integer, nullable=True)
    iv = Column(Numeric, nullable=True) # Implied Volatility
    oi = Column(Numeric, nullable=True) # Open Interest
    delta = Column(Numeric(3, 2), nullable=True)
    gamma = Column(Numeric(3, 2), nullable=True)
    theta = Column(Numeric(3, 2), nullable=True)
    vega = Column(Numeric(3, 2), nullable=True)

    tick_ref = relationship("Ticks", back_populates="greeks") # Relationship back to Ticks
    bar_ref = relationship("Bars", back_populates="greeks") # Relationship back to Bars

    __table_args__ = (
        # Index for Greeks queries by related tick or bar
        Index('idx_greeks_tick_id', 'tick_id'),
        Index('idx_greeks_bar_id', 'bar_id'),
    )


class Trades(Base):
    __tablename__ = 'trades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    algo = Column(String(32), nullable=True, index=True)
    symbol = Column(String(12), nullable=True, index=True) # Note: Consider if this should be a ForeignKey to Symbols
    direction = Column(String(5), nullable=True)
    quantity = Column(Integer, nullable=True)
    # Consider TIMESTAMP(precision=6) if needed
    entry_time = Column(DateTime(timezone=False), nullable=True, index=True)
    exit_time = Column(DateTime(timezone=False), nullable=True, index=True)
    exit_reason = Column(String(8), nullable=True, index=True)
    order_type = Column(String(6), nullable=True, index=True)
    market_price = Column(Numeric, nullable=True, index=True)
    target = Column(Numeric, nullable=True)
    stop = Column(Numeric, nullable=True)
    entry_price = Column(Numeric, nullable=True, index=True)
    exit_price = Column(Numeric, nullable=True, index=True)
    realized_pnl = Column(Numeric, nullable=False, server_default='0')

    __table_args__ = (
        # Index for trade queries by algo and time range
        Index('idx_trades_algo_entry_time', 'algo', 'entry_time'),
        Index('idx_trades_algo_exit_time', 'algo', 'exit_time'),
        # Index for time-based queries across all algos
        Index('idx_trades_entry_time_brin', 'entry_time', postgresql_using='brin'),
        Index('idx_trades_exit_time_brin', 'exit_time', postgresql_using='brin'),
        # Composite index for symbol-based queries
        Index('idx_trades_symbol_entry_time', 'symbol', 'entry_time'),
        # Unique constraint to prevent duplicate trade entries
        UniqueConstraint('algo', 'symbol', 'entry_time', name='uq_trade_algo_symbol_entry'),
    )
