
import pandas as pd
import pytz
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import logging

# Initialize logger for this module
_logger = logging.getLogger('quant_async.util')


# ---------------------------------------------
def prepare_history(data: List[Dict[str, Any]], resolution: str = '1T', 
                   tz: str = 'UTC', table_type: str = 'bars') -> pd.DataFrame:
    """
    Prepare historical data from database records into a pandas DataFrame.
    
    This function converts raw database records into a properly formatted DataFrame
    with datetime index and appropriate column types for quantitative analysis.
    
    Args:
        data: List of database records (dictionaries)
        resolution: Time resolution for the data ('1T', '5T', '1H', '1D', etc.)
        tz: Target timezone for datetime conversion
        table_type: Type of data table ('bars', 'ticks', 'greeks')
        
    Returns:
        pandas.DataFrame: Formatted DataFrame with datetime index
        
    Raises:
        ValueError: If data format is invalid or timezone conversion fails
    """
    try:
        if not data:
            _logger.info("No data provided to prepare_history, returning empty DataFrame")
            return pd.DataFrame()
        
        # Convert list of records to DataFrame
        df = pd.DataFrame(data)
        
        if df.empty:
            _logger.info("Empty data converted to DataFrame")
            return df
        
        # Ensure datetime column exists
        if 'datetime' not in df.columns:
            raise ValueError("Missing 'datetime' column in historical data")
        
        # Convert datetime column to pandas datetime with timezone awareness
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        
        # Handle timezone conversion
        if tz != 'UTC':
            try:
                target_tz = pytz.timezone(tz)
                df['datetime'] = df['datetime'].dt.tz_convert(target_tz)
            except Exception as e:
                _logger.warning(f"Failed to convert timezone to {tz}: {e}. Using UTC.")
        
        # Set datetime as index (standard for time-series analysis)
        df.set_index('datetime', inplace=True)
        
        # Sort by datetime to ensure chronological order
        df.sort_index(inplace=True)
        
        # Handle data type conversions based on table type
        if table_type == 'bars':
            # Convert OHLCV columns to appropriate numeric types
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('int64')
        
        elif table_type == 'ticks':
            # Convert tick price and size columns
            price_columns = ['bid', 'ask', 'last']
            size_columns = ['bidsize', 'asksize', 'lastsize']
            
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            for col in size_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('int64')
        
        elif table_type == 'greeks':
            # Convert Greeks columns to numeric
            greeks_columns = ['price', 'underlying', 'dividend', 'iv', 'oi', 
                            'delta', 'gamma', 'theta', 'vega']
            for col in greeks_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('int64')
        
        # Remove any columns that are not useful for analysis
        columns_to_drop = ['id', 'symbol_id']
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], 
                inplace=True, errors='ignore')
        
        # Log the successful preparation
        _logger.info(f"Successfully prepared {len(df)} rows of {table_type} data "
                    f"for resolution {resolution} in timezone {tz}")
        
        return df
        
    except Exception as e:
        error_msg = f"Failed to prepare historical data: {str(e)}"
        _logger.error(error_msg)
        raise ValueError(error_msg)


# ---------------------------------------------
def validate_time_range(start: Union[str, datetime], end: Optional[Union[str, datetime]] = None,
                        max_days: int = 365) -> tuple[datetime, datetime]:
    """
    Validate and parse time range for historical data queries.
    
    Args:
        start: Start date/time (string or datetime object)
        end: End date/time (optional, defaults to now)
        max_days: Maximum allowed days in the range
        
    Returns:
        tuple: (start_datetime, end_datetime) as timezone-aware datetimes
        
    Raises:
        ValueError: If time range is invalid or too large
    """
    try:
        # Parse start time
        if isinstance(start, str):
            start_dt = pd.to_datetime(start)
        else:
            start_dt = start
        
        # Parse end time
        if end is None:
            end_dt = datetime.now(pytz.UTC)
        elif isinstance(end, str):
            end_dt = pd.to_datetime(end)
        else:
            end_dt = end
        
        # Ensure timezone awareness
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=pytz.UTC)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=pytz.UTC)
        
        # Validate time range
        if start_dt >= end_dt:
            raise ValueError(f"Start time ({start_dt}) must be before end time ({end_dt})")
        
        # Check if range is too large
        duration = (end_dt - start_dt).days
        if duration > max_days:
            raise ValueError(f"Time range ({duration} days) exceeds maximum allowed ({max_days} days)")
        
        return start_dt, end_dt
        
    except Exception as e:
        raise ValueError(f"Invalid time range: {str(e)}")


# ---------------------------------------------
def convert_resolution_to_timedelta(resolution: str) -> pd.Timedelta:
    """
    Convert resolution string to pandas Timedelta for data aggregation.
    
    Args:
        resolution: Resolution string ('1T', '5T', '1H', '1D', etc.)
        
    Returns:
        pandas.Timedelta: Timedelta object for the resolution
        
    Raises:
        ValueError: If resolution format is invalid
    """
    try:
        # Handle common resolution formats
        resolution_map = {
            '1T': '1min', '5T': '5min', '15T': '15min', '30T': '30min',
            '1H': '1H', '4H': '4H', '1D': '1D', '1W': '1W', '1M': '1M'
        }
        
        if resolution in resolution_map:
            return pd.Timedelta(resolution_map[resolution])
        else:
            # Try to parse directly
            return pd.Timedelta(resolution)
            
    except Exception as e:
        raise ValueError(f"Invalid resolution format '{resolution}': {str(e)}")


# ---------------------------------------------
def aggregate_ticks_to_bars(tick_df: pd.DataFrame, resolution: str = '1T') -> pd.DataFrame:
    """
    Aggregate tick data into OHLCV bars.
    
    Args:
        tick_df: DataFrame with tick data (must have 'last' prices)
        resolution: Target bar resolution
        
    Returns:
        pandas.DataFrame: Aggregated OHLCV bars
        
    Raises:
        ValueError: If tick data is insufficient for aggregation
    """
    try:
        if tick_df.empty:
            return pd.DataFrame()
        
        if 'last' not in tick_df.columns:
            raise ValueError("Tick data must contain 'last' price column for aggregation")
        
        # Remove rows with NaN last prices
        tick_df = tick_df.dropna(subset=['last'])
        
        if tick_df.empty:
            return pd.DataFrame()
        
        # Resample to create OHLCV bars
        timedelta = convert_resolution_to_timedelta(resolution)
        
        # Group by the resolution period
        bars = tick_df.resample(timedelta).agg({
            'last': ['first', 'max', 'min', 'last'],  # OHLC from last prices
            'lastsize': 'sum'  # Volume from last sizes
        }).dropna()
        
        # Flatten column names
        bars.columns = ['open', 'high', 'low', 'close', 'volume']
        
        # Ensure volume is integer
        bars['volume'] = bars['volume'].astype(int)
        
        _logger.info(f"Aggregated {len(tick_df)} ticks into {len(bars)} {resolution} bars")
        
        return bars
        
    except Exception as e:
        error_msg = f"Failed to aggregate ticks to bars: {str(e)}"
        _logger.error(error_msg)
        raise ValueError(error_msg)


# ---------------------------------------------
def create_ib_tuple(instrument):
    """ create ib contract tuple """
    from quant_async import futures

    if isinstance(instrument, str):
        instrument = instrument.upper()

        if "FUT." not in instrument:
            # symbol stock
            instrument = (instrument, "STK", "SMART", "USD", "", 0.0, "")

        else:
            # future contract
            try:
                symdata = instrument.split(".")

                # is this a CME future?
                if symdata[1] not in futures.futures_contracts.keys():
                    raise ValueError(
                        "Un-supported symbol. Please use full contract tuple.")

                # auto get contract details
                spec = futures.get_ib_futures(symdata[1])
                if not isinstance(spec, dict):
                    raise ValueError("Un-parsable contract tuple")

                # expiry specified?
                if len(symdata) == 3 and symdata[2] != '':
                    expiry = symdata[2]
                else:
                    # default to most active
                    expiry = futures.get_active_contract(symdata[1])

                instrument = (spec['symbol'].upper(), "FUT",
                              spec['exchange'].upper(), spec['currency'].upper(),
                              int(expiry), 0.0, "")

            except Exception:
                raise ValueError("Un-parsable contract tuple")

    # tuples without strike/right
    elif len(instrument) <= 7:
        instrument_list = list(instrument)
        if len(instrument_list) < 3:
            instrument_list.append("SMART")
        if len(instrument_list) < 4:
            instrument_list.append("USD")
        if len(instrument_list) < 5:
            instrument_list.append("")
        if len(instrument_list) < 6:
            instrument_list.append(0.0)
        if len(instrument_list) < 7:
            instrument_list.append("")

        try:
            instrument_list[4] = int(instrument_list[4])
        except Exception:
            pass

        instrument_list[5] = 0. if isinstance(instrument_list[5], str) \
            else float(instrument_list[5])

        instrument = tuple(instrument_list)

    return instrument