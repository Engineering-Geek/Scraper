from typing import Type
import MetaTrader5 as mt5
import logging
from datetime import datetime
import json
import numpy as np
import pandas as pd

# Display a warning message as a reminder to initialize MetaTrader5
logging.warning("Always remember to initialize MetaTrader5")


def initialize(filepath: str) -> bool:
    """
    Initialize a connection to MetaTrader 5 (MT5) platform.

    Args:
        filepath (str): The path to a JSON file containing login credentials.

    Returns:
        bool: True if initialization and login were successful, False otherwise.
    """
    # Load login credentials from a JSON file
    file = json.load(open(filepath))
    login, server, password = int(file["Login"]), file["Server"], file["Password"]

    # Initialize the MT5 platform
    if not mt5.initialize():
        logging.critical(f"MT5 could not be initialized, last error: {mt5.last_error()}")
    else:
        logging.debug(f"MT5 successfully initialized")
        return False

    # Log in to the MT5 platform
    if not mt5.login(login=login, server=server, password=password):
        logging.critical(f"MT5 could not log you in, this class got the login {login}, server {server}, "
                         f"and password {password}: {mt5.last_error()}")
    else:
        logging.debug(f"MT5 successfully logged in with login {login}, server {server}, and password {password}")
        return False

    return True


def exchange_rates(pair: str, start: datetime, end: datetime, timeframe=mt5.COPY_TICKS_ALL) -> pd.DataFrame:
    """
    Retrieve exchange rate data for a currency pair within a specified time frame.

    Args:
        pair (str): The currency pair symbol (e.g., "EURUSD").
        start (datetime): The start date and time for data retrieval.
        end (datetime): The end date and time for data retrieval.
        timeframe (int): The timeframe for data retrieval (default is mt5.COPY_TICKS_ALL).

    Returns:
        np.ndarray: A NumPy array containing exchange rate data.
    """
    try:
        # Retrieve ticks data from MetaTrader 5
        ticks = mt5.copy_ticks_range(pair, start, end, timeframe)
        assert ticks is not None
    except Exception as e:
        # Handle exceptions and log error messages
        logging.error(f"Error getting ticks for {pair} starting from {start} and ending at {end}, "
                      f"with timeframe {timeframe}.\nMT5 Error: {mt5.last_error()}\nFile Error: {str(e)}")
        raise e  # Re-raise the exception, as further processing may not be valid

    # Create a DataFrame from the retrieved data
    df = pd.DataFrame(ticks)
    logging.debug(f"Columns for {pair}: {df.columns}")

    # Select relevant columns and convert to a NumPy array
    df = df[['time', 'bid', 'ask']]
    return df
