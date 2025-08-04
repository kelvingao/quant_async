# Quant Async

![Python version](https://img.shields.io/badge/python-3.12+-blue.svg?style=flat)
![PyPi version](https://img.shields.io/pypi/v/quant_async.svg?maxAge=60)
![PyPi status](https://img.shields.io/pypi/status/quant_async.svg?maxAge=60)
![Coverage](https://img.shields.io/badge/coverage-37%25-red)

**An Interactive Brokers Async Trading Framework for Python 3.12+**

Quant Async is a high-performance, production-ready framework for algorithmic trading with Interactive Brokers. It provides real-time market data streaming, persistent data storage, and a clean async API for building sophisticated trading strategies.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ZeroMQ     ┌─────────────────┐
│   Algorithm 1   │◄──────────────┤                 │
└─────────────────┘               │                 │
┌─────────────────┐    Pub/Sub    │     Blotter     │    IB API    ┌─────────────────┐
│   Algorithm 2   │◄──────────────┤  (Data Stream)  │◄─────────────┤ Interactive     │
└─────────────────┘               │                 │              │ Brokers         │
┌─────────────────┐               │                 │              │ TWS/Gateway     │
│   Algorithm N   │◄──────────────┤                 │              └─────────────────┘
└─────────────────┘               └─────────────────┘
                                           │
                                    PostgreSQL
                                  ┌─────────────────┐
                                  │  Market Data    │
                                  │   Database      │
                                  │                 │
                                  │ • Ticks         │
                                  │ • Bars          │  
                                  │ • Quotes        │
                                  │ • Greeks        │
                                  │ • Trades        │
                                  └─────────────────┘
```

### Core Components

- **Blotter**: Continuous market data collector and broadcaster
- **Algo**: Base class for trading algorithms with real-time data consumption  
- **Broker**: Interactive Brokers connection and order management
- **Database**: PostgreSQL storage with asyncpg for historical data and backtesting
- **Streaming**: ZeroMQ pub/sub for real-time data distribution
- **Dashboard**: FastAPI web interface for monitoring and control

## 🚀 Features

- ✅ **Continuous Data Collection**: 24/7 market data capture independent of strategy execution
- ✅ **Real-time Streaming**: ZeroMQ pub/sub architecture supports multiple concurrent algorithms
- ✅ **Database Persistence**: All tick, bar, quote, and Greek data stored in PostgreSQL
- ✅ **Hot Symbol Management**: CSV-based symbol management with live reload
- ✅ **Async Architecture**: Built on asyncio for high-performance concurrent operations
- ✅ **Production Ready**: Comprehensive error handling, logging, and monitoring
- ✅ **Web Dashboard**: FastAPI interface for system monitoring and control
- ✅ **Historical Analysis**: Efficient data retrieval for backtesting and research

## 📋 Requirements

- **Python**: 3.12+
- **Interactive Brokers**: TWS or IB Gateway
- **PostgreSQL**: 12+ (for data persistence)
- **Operating System**: macOS, Linux

## 🛠️ Installation

### 1. Clone Repository

```bash
git clone https://github.com/kelvingao/quant_async.git
cd quant_async
```

### 2. Install Dependencies

Using `uv` (recommended):
```bash
uv sync
uv sync --group dev  # Include development dependencies
```

Using `pip`:
```bash
pip install -e .
pip install -e .[dev]  # Include development dependencies
```

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env with your specific configuration
```

### 4. Setup PostgreSQL Database

```bash
# Connect to PostgreSQL as superuser
psql -U postgres

# Create database and user
CREATE DATABASE quant_async;
CREATE USER quant_async WITH PASSWORD 'quant_async';
GRANT ALL PRIVILEGES ON DATABASE quant_async TO quant_async;

# Connect to the new database  
\c quant_async

# Grant schema permissions
GRANT CREATE ON SCHEMA public TO quant_async;
```

### 5. Run Database Migrations

```bash
uv run alembic upgrade head
```

### 6. Setup Interactive Brokers

#### TWS (Trader Workstation)
1. Download and install TWS from Interactive Brokers
2. Enable API access: `Configure → API → Enable ActiveX and Socket Clients`
3. Set API port (7497 for live, 7496 for paper trading)
4. Disable "Read-Only API"

#### IB Gateway (Recommended for Production)
1. Download IB Gateway
2. Configure API settings (port 4001 for live, 4002 for paper)
3. Enable API access and disable read-only mode

## 🏃‍♂️ Quick Start

### 1. Configure Symbols

Edit `examples/symbols.csv` to specify instruments to monitor:

```csv
symbol,sec_type,exchange,currency,expiry,strike,opt_type
NVDA,STK,SMART,USD,,,
AAPL,STK,SMART,USD,,,
ES,FUT,CME,USD,20250919,,
EURUSD,CASH,IDEALPRO,USD,,,
```

### 2. Start the Blotter

```bash
# Start continuous market data collection
uv run python examples/blotter.py

# With custom configuration
uv run python examples/blotter.py --ibport=4002 --dbname=quant_async_paper
```

### 3. Start the Dashboard

```bash
# Launch web interface (http://localhost:5002)
uv run python examples/dashboard.py
```

### 4. Run an Algorithm

```bash
# Run example trading strategy
uv run python examples/strategy.py
```

## 📊 Usage Examples

### Basic Blotter Usage

```python
import asyncio
from quant_async import Blotter, util

class MyBlotter(Blotter):
    pass

if __name__ == "__main__":
    util.logToConsole("INFO")
    
    blotter = MyBlotter(
        symbols="symbols.csv",
        ibhost="localhost", 
        ibport=4001,
        dbname="quant_async"
    )
    
    asyncio.run(blotter.run())
```

### Algorithm Development

```python
from quant_async import Algo

class MovingAverageStrategy(Algo):
    
    def __init__(self):
        super().__init__(
            instruments=[("AAPL", "STK", "SMART", "USD")],
            ibclient=998
        )
        self.sma_short = 10
        self.sma_long = 20
    
    def on_start(self):
        print("Strategy starting...")
    
    def on_quote(self, instrument):
        # Process real-time quotes
        quote_data = self.quotes[instrument.symbol]
        print(f"Quote for {instrument.symbol}: {quote_data}")
    
    def on_tick(self, instrument):
        # Process tick data
        print(f"Tick for {instrument.symbol}")
    
    def on_bar(self, instrument):
        # Process bar data for strategy logic
        print(f"Bar for {instrument.symbol}")

if __name__ == "__main__":
    strategy = MovingAverageStrategy()
    asyncio.run(strategy.run())
```

### Multiple Algorithm Example

```python
# Run multiple strategies consuming the same data stream
from quant_async import Algo
import asyncio

class MomentumStrategy(Algo):
    def __init__(self):
        super().__init__(
            instruments=[("NVDA", "STK", "SMART", "USD")],
            ibclient=999
        )
    
    def on_quote(self, instrument):
        # Momentum strategy logic
        pass

class MeanReversionStrategy(Algo):
    def __init__(self):
        super().__init__(
            instruments=[("AAPL", "STK", "SMART", "USD")],
            ibclient=1000
        )
    
    def on_quote(self, instrument):
        # Mean reversion strategy logic  
        pass

async def run_multiple_strategies():
    momentum = MomentumStrategy()
    mean_reversion = MeanReversionStrategy()
    
    # Run both strategies concurrently
    await asyncio.gather(
        momentum.run(),
        mean_reversion.run()
    )

if __name__ == "__main__":
    asyncio.run(run_multiple_strategies())
```

## 🔧 Configuration

### Environment Variables

The framework supports configuration via environment variables. Copy `.env.example` to `.env` and customize:

```bash
# Interactive Brokers
IB_HOST=localhost
IB_PORT=4001
IB_CLIENT_ID=996

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=quant_async
DB_USER=quant_async
DB_PASS=quant_async

# ZeroMQ
ZMQ_PORT=12345

# Logging
LOG_LEVEL=INFO
```

### Command Line Arguments

All components support command-line configuration:

```bash
# Blotter options
python examples/blotter.py \
    --ibhost localhost \
    --ibport 4001 \
    --ibclient 996 \
    --dbhost localhost \
    --dbname quant_async \
    --zmqport 12345 \
    --symbols symbols.csv

# Algorithm options  
python examples/strategy.py \
    --ibhost localhost \
    --ibport 4001 \
    --ibclient 998 \
    --blotter auto-detect
```

## 🗄️ Database Schema

The framework uses PostgreSQL with the following tables:

- **symbols**: Instrument definitions and metadata
- **bars**: OHLCV bar data with volume
- **ticks**: Real-time tick data (bid/ask/last)
- **greeks**: Options Greeks (delta, gamma, theta, vega)
- **trades**: Executed trade records

### Database Queries

```python
# Historical data retrieval
from quant_async import Blotter

blotter = Blotter(dbskip=False)
await blotter.postgres_connect()

# Get historical bars
bars = await blotter.history(
    symbols=["AAPL_STK"], 
    start="2024-01-01",
    end="2024-12-31",
    resolution="1D"
)

# Get tick data
ticks = await blotter.history(
    symbols=["EURUSD_CASH"],
    start="2024-08-01", 
    resolution="1T"  # 1 tick resolution
)
```

## 📊 Monitoring & Dashboard

### Web Dashboard

Access the web interface at `http://localhost:5002`:

- Real-time market data display
- System health monitoring  
- Connection status indicators
- Performance metrics
- Historical data charts

### Health Checks

```bash
# Check system status
curl http://localhost:8080/health

# Get metrics
curl http://localhost:8080/metrics
```

### Logging

The framework uses structured logging:

```python
import logging
from quant_async import util

# Configure logging
util.logToConsole("INFO")

# Custom logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading.log'),
        logging.StreamHandler()
    ]
)
```

## 🧪 Testing

The project includes comprehensive unit tests and integration tests.

### Unit Tests

Run unit tests (no external dependencies required):

```bash
# Run all unit tests (excludes integration tests)
uv run pytest tests/ -m "not integration" -v

# Run specific test modules
uv run pytest tests/test_blotter.py -v
uv run pytest tests/test_streaming.py -v
uv run pytest tests/test_database.py -v

# Run with coverage and update badge
./scripts/update_coverage.sh

# Or run coverage manually
uv run pytest tests/ -m "not integration" --cov=src/quant_async --cov-report=html --cov-report=json

# Update coverage badge in README
python scripts/generate_coverage_badge.py
```

### Integration Tests

Integration tests require a real PostgreSQL database. Set up the test environment:

```bash
# 1. Copy environment template
cp .env.example .env

# 2. Create test database (as postgres user)
sudo -u postgres psql -f setup_integration_tests.sql

# 3. Configure .env file with test database settings:
# TEST_DB_HOST=localhost
# TEST_DB_PORT=5432
# TEST_DB_USER=quant_async
# TEST_DB_PASS=quant_async
# TEST_DB_NAME=quant_async_test

# 4. Run integration tests
uv run pytest tests/ -m integration -v

# 5. Run all tests (unit + integration)
uv run pytest tests/ -v
```

**Integration Test Markers:**
- `@pytest.mark.integration` - Requires real PostgreSQL database
- Tests are automatically skipped if database is not available

## 🚨 Troubleshooting

### Common Issues

#### IB Connection Failed
```
Error: Cannot connect to Interactive Brokers
```
**Solutions:**
- Verify TWS/Gateway is running
- Check API settings are enabled
- Confirm correct port (7497/4001 live, 7496/4002 paper)
- Ensure client ID is unique
- Check firewall settings

#### Database Connection Error  
```
Error: Cannot connect to PostgreSQL
```
**Solutions:**
- Verify PostgreSQL is running
- Check database credentials in .env
- Ensure database exists and user has permissions
- Test connection: `psql -U quant_async -d quant_async -h localhost`

#### ZeroMQ Port Conflict
```
Error: Address already in use
```
**Solutions:**
- Change ZMQ_PORT in .env
- Kill existing process: `lsof -ti:12345 | xargs kill -9`
- Use different port for each Blotter instance

#### Symbol File Issues
```
Error: Symbol file not found or invalid
```
**Solutions:**
- Verify symbols.csv exists and is readable
- Check CSV format matches expected columns
- Ensure file permissions (0o666)
- Review example symbols.csv for format

### Debug Mode

Enable verbose logging for troubleshooting:

```python
from quant_async import util
util.logToConsole("DEBUG")
```

### Performance Tuning

For high-frequency trading:

```python
# Optimize database connections
blotter = Blotter(
    dbhost="localhost",
    # Increase connection pool size
    # Configure in get_postgres_connection()
)

# Reduce ZeroMQ latency
# Use dedicated network interface
# Increase system buffer sizes
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make changes and add tests
4. Run tests: `uv run pytest tests/`
5. Run linting: `uv run ruff check src/ tests/`
6. Commit changes: `git commit -m 'Add new feature'`
7. Push to branch: `git push origin feature/new-feature`
8. Submit a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.