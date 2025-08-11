import pandas as pd
import pandas_ta as ta

# Ensure DataFrame has the required OHLC columns, optionally Volume
# For pandas_ta, columns are often expected to be lowercase: 'open', 'high', 'low', 'close', 'volume'

def calculate_ema(ohlc_df: pd.DataFrame, length: int = 20, source_col: str = 'close') -> pd.Series:
    """Calculates Exponential Moving Average (EMA)."""
    if ohlc_df is None or ohlc_df.empty or source_col not in ohlc_df.columns:
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length: # Not enough data for EMA
        return pd.Series(dtype='float64')
    return ohlc_df.ta.ema(length=length, append=False)

def calculate_atr(ohlc_df: pd.DataFrame, length: int = 14) -> pd.Series:
    """Calculates Average True Range (ATR). Requires 'high', 'low', 'close' columns."""
    if ohlc_df is None or ohlc_df.empty or not all(col in ohlc_df.columns for col in ['high', 'low', 'close']):
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length: # Not enough data for ATR
        return pd.Series(dtype='float64')
    # pandas_ta expects lowercase column names
    temp_df = ohlc_df.rename(columns={'high': 'high', 'low': 'low', 'close': 'close'})
    return temp_df.ta.atr(length=length, append=False)

def calculate_rsi(ohlc_df: pd.DataFrame, length: int = 14, source_col: str = 'close') -> pd.Series:
    """Calculates Relative Strength Index (RSI)."""
    if ohlc_df is None or ohlc_df.empty or source_col not in ohlc_df.columns:
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length:
        return pd.Series(dtype='float64')
    return ohlc_df.ta.rsi(length=length, close=ohlc_df[source_col], append=False)

def calculate_adx(ohlc_df: pd.DataFrame, length: int = 14) -> pd.Series:
    """
    Calculates Average Directional Index (ADX).
    Requires 'high', 'low', 'close'.
    Returns a Series with the ADX values.
    """
    if ohlc_df is None or ohlc_df.empty or not all(col in ohlc_df.columns for col in ['high', 'low', 'close']):
        return pd.Series(dtype='float64')
    if len(ohlc_df) < length * 2: # ADX needs more data than length
        return pd.Series(dtype='float64')

    adx_df = ohlc_df.ta.adx(length=length, append=False)
    if adx_df is None or adx_df.empty:
        return pd.Series(dtype='float64')

    # ta.adx returns a DataFrame with ADX, DMP, DMN. We only need the ADX column.
    adx_series = adx_df.iloc[:, 0] # ADX is typically the first column
    return adx_series

if __name__ == '__main__':
    # Example Usage (requires a sample CSV or DataFrame)
    # Create a sample DataFrame for testing
    data = {
        'timestamp': pd.to_datetime(['2023-01-01 10:00:00', '2023-01-01 10:01:00', '2023-01-01 10:02:00',
                                     '2023-01-01 10:03:00', '2023-01-01 10:04:00', '2023-01-01 10:05:00',
                                     '2023-01-01 10:06:00', '2023-01-01 10:07:00', '2023-01-01 10:08:00',
                                     '2023-01-01 10:09:00', '2023-01-01 10:10:00']),
        'open':  [1.1000, 1.1002, 1.1005, 1.1003, 1.1008, 1.1010, 1.1009, 1.1012, 1.1015, 1.1013, 1.1018],
        'high':  [1.1003, 1.1006, 1.1007, 1.1010, 1.1012, 1.1015, 1.1013, 1.1016, 1.1018, 1.1017, 1.1020],
        'low':   [1.0999, 1.1001, 1.1002, 1.1001, 1.1007, 1.1008, 1.1007, 1.1010, 1.1012, 1.1011, 1.1014],
        'close': [1.1002, 1.1005, 1.1003, 1.1008, 1.1010, 1.1009, 1.1012, 1.1015, 1.1013, 1.1018, 1.1016],
        'volume':[10,     12,     15,     13,     18,     20,     19,     22,     25,     23,     28]
    }
    sample_ohlc_df = pd.DataFrame(data)
    sample_ohlc_df.set_index('timestamp', inplace=True) # pandas-ta often works well with DatetimeIndex

    print("Sample OHLC Data:")
    print(sample_ohlc_df)
    print("\nEMA (10) on close:")
    ema_values = calculate_ema(sample_ohlc_df, length=5, source_col='close')
    print(ema_values)

    print("\nATR (5):")
    # Ensure columns are lowercase if pandas-ta expects it, or pass a renamed df
    atr_values = calculate_atr(sample_ohlc_df.rename(columns=str.lower), length=5)
    print(atr_values)

    print("\nRSI (7) on close:")
    rsi_values = calculate_rsi(sample_ohlc_df, length=7, source_col='close')
    print(rsi_values)

    print("\nStochastic (5,3,3):")
    stoch_values = calculate_stochastic(sample_ohlc_df.rename(columns=str.lower), k=5, d=3, smooth_k=3)
    print(stoch_values)

    print("\nMomentum (6) on close:")
    mom_values = calculate_momentum(sample_ohlc_df, length=6, source_col='close')
    print(mom_values)

    print("\nDonchian Channels (5,5):")
    donchian_values = calculate_donchian(sample_ohlc_df.rename(columns=str.lower), lower_length=5, upper_length=5)
    print(donchian_values)

    print("\nBollinger Bands (5,2) on close:")
    bbands_values = calculate_bollinger_bands(sample_ohlc_df, length=5, std=2, source_col='close')
    print(bbands_values)

    # Test with insufficient data
    print("\nEMA (10) on close (insufficient data):")
    ema_insufficient = calculate_ema(sample_ohlc_df.head(3), length=5)
    print(ema_insufficient) # Should be empty or all NaN

    print("\nATR (14) with insufficient data")
    atr_insufficient = calculate_atr(sample_ohlc_df.head(10).rename(columns=str.lower), length=14)
    print(atr_insufficient)

    print("\nTesting with empty DataFrame:")
    empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    print("EMA from empty:", calculate_ema(empty_df, 5))
    print("ATR from empty:", calculate_atr(empty_df, 5))

    # Note: pandas-ta often returns Series/DataFrames with NaNs at the beginning
    # where the indicator cannot be calculated due to insufficient prior data.
    # This is standard behavior.
    # The wrappers include basic checks for empty DFs or insufficient length for the period.
    # pandas-ta also expects column names to be lowercase by default for many indicators,
    # so the wrappers use .rename(columns=str.lower) or pass specific column names.
    # For the real OHLC data from Trader, we will ensure columns are named 'open', 'high', 'low', 'close'.

    # The `append=False` argument in pandas_ta calls ensures that the indicator results
    # are returned as new Series/DataFrames rather than appending to the input DataFrame.
    # This is generally cleaner for our wrapper functions.
    pass # End of example usage
