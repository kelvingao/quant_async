from typing import Any
from sqlalchemy import (
    Column, Integer, BigInteger, String, Date, DateTime, Numeric,
    ForeignKey, UniqueConstraint
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
        UniqueConstraint('datetime', 'symbol_id', name='uq_bar_datetime_symbol'),
    )

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
        UniqueConstraint('datetime', 'symbol_id', name='uq_tick_datetime_symbol'),
    )

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
        UniqueConstraint('algo', 'symbol', 'entry_time', name='uq_trade_algo_symbol_entry'),
    )
