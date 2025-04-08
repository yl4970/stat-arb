import pandas as pd
import numpy as np
from tqdm import tqdm

def apply_to_data(func):
    """
    A decorator to apply a function to either a dictionary of DataFrames or a single DataFrame.

    Parameters:
        func (function): A function that operates on a single DataFrame.

    Returns:
        function: A wrapped function that can handle both dictionaries and single DataFrames.
    """
    def wrapper(data, *args, **kwargs):
        if isinstance(data, dict):
            # Apply the function to each DataFrame in the dictionary
            return {key: func(df, *args, **kwargs) for key, df in tqdm(data.items(), desc="Processing DataFrames")}
        else:
            # Apply the function to a single DataFrame
            return func(data, *args, **kwargs)
    return wrapper


@apply_to_data
def load_all(df, threshold=50, latency=10, transaction_fee=50):
    df = filter_trading_hours(df)
    df = filter_na_exchange(df)
    df = filter_actions(df)
    df = reshape_bid_ask_data(df)
    return process_signals(df, threshold=threshold, latency=latency, transaction_fee=transaction_fee)


@apply_to_data
def filter_trading_hours(df):
    """Filters rows within trading hours (9:30 AM to 4:00 PM)."""
    market_open = 93000000  # 9:30 AM
    market_close = 160000000  # 4:00 PM
    return df[(df['Timestamp'] >= market_open) & (df['Timestamp'] <= market_close)]


@apply_to_data
def filter_na_exchange(df):
    """Filters rows where 'Exchange' is NaN."""
    return df[~df['Exchange'].isna()]


@apply_to_data
def filter_actions(df, action_type = "FQ"):
    """
    Filters rows based on the action type, defaulting to "FQ" as in FirmQuote.
    """
    return df[df['Action'].str.contains(action_type, na=False)]


@apply_to_data
def reshape_bid_ask_data(df):
    """
    Reshapes the DataFrame to pivot bid and ask data for easier analysis.
    """
    pivoted_df = df.pivot_table(
    index=['Timestamp'],
    columns='Side',
        values=[col for col in df.columns if col not in ['Date', 'Timestamp', 'Side']],
        aggfunc='first'
)
    pivoted_df[('Price', 'A')] = pivoted_df[('Price', 'A')] * (-1)
    return pivoted_df


@apply_to_data
def process_signals(df, threshold=50, latency=10, transaction_fee=50):
    """
    Processes trading signals by identifying exploitable price differences based on bid/ask data.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing trading data.
        threshold (int): The minimum quantity threshold for a valid signal.
        latency (int): The minimum duration (in rows) for a valid signal.
        transaction_fee (float): The transaction fee per contract.

    Returns:
        dict: A dictionary containing identified signals with relevant details.
    """
    # Initialize variables
    ex_dict = {}
    latest_price = {}
    exchange_a = df[('Exchange', 'A')]
    exchange_b = df[('Exchange', 'B')]
    price_a = df[('Price', 'A')]
    price_b = df[('Price', 'B')]
    quantity_a = df[('Quantity', 'A')]
    quantity_b = df[('Quantity', 'B')]

    def get_signal(signal_start, signal_end, signal_side, sum_q, current_ex, latest_price):
        """
        Identifies if there is an exploitable price difference based on the signal side and other side prices.

        Parameters:
            signal_start (int): The starting index of the signal.
            signal_end (int): The ending index of the signal.
            signal_side (str): The side of the signal ('A' or 'B').
            sum_q (float): The total quantity for the signal side.
            current_ex (str): The current exchange.
            latest_price (dict): A dictionary of the latest prices for each exchange and side.

        Returns:
            dict: A dictionary containing the signal details if exploitable, otherwise an empty dictionary.
        """
        other_side = 'B' if signal_side == 'A' else 'A'
        other_side_price = {
            key: latest_price[key]
            for key in latest_price
            if key[1] == other_side and key[0] != current_ex
        }

        # Check if latency is viable
        if signal_end - signal_start >= latency:
            current_price = df[('Price', signal_side)].iloc[signal_end]
            if signal_side == 'A':
                # Other side should have a high enough sell price for price difference to be exploitable
                stale_price = max(other_side_price.values(), default=np.nan)
                stale_exchange = max(other_side_price, key=other_side_price.get, default=None)
            else:
                # Vice versa
                stale_price = min(other_side_price.values(), default=np.nan)
                stale_exchange = min(other_side_price, key=other_side_price.get, default=None)

            # Check if the price difference is greater than the transaction fee
            if not np.isnan(stale_price) and current_price + stale_price > transaction_fee / 100:
                key = (
                    df.iloc[signal_start].name,
                    df.iloc[signal_end].name,
                    current_ex,
                )
                return {
                    key: [
                        signal_end - signal_start,
                        current_price + stale_price - transaction_fee/100,
                        sum_q,
                        stale_exchange,
                    ]
                }
        return {}

    # Main loop
    i = 0
    while i < len(df):
        signal_start = i
        current_ex = exchange_a.iloc[i] if pd.isna(exchange_b.iloc[i]) else exchange_b.iloc[i]
        latest_price[(current_ex, 'A')] = price_a.iloc[i] if not pd.isna(price_a.iloc[i]) else latest_price.get((current_ex, 'A'), np.nan)
        latest_price[(current_ex, 'B')] = price_b.iloc[i] if not pd.isna(price_b.iloc[i]) else latest_price.get((current_ex, 'B'), np.nan)
        sum_q_a = 0
        sum_q_b = 0

        # Group consecutive rows by the same exchange
        j = i
        while j < len(df) and (exchange_a.iloc[j] == current_ex or exchange_b.iloc[j] == current_ex):
            sum_q_a += quantity_a.iloc[j]
            sum_q_b += quantity_b.iloc[j]
            j += 1

        # Marking the end of the signal
        signal_end = j - 1

        # Identify the side of the purchase pressure
        if sum_q_a >= threshold and (pd.isna(sum_q_b) or sum_q_b < threshold):
            ex_dict.update(get_signal(signal_start, signal_end, 'A', sum_q_a, current_ex, latest_price))
        elif sum_q_b >= threshold and (pd.isna(sum_q_a) or sum_q_a < threshold):
            ex_dict.update(get_signal(signal_start, signal_end, 'B', sum_q_b, current_ex, latest_price))
        elif sum_q_a >= threshold and sum_q_b >= threshold:
            ex_dict.update(get_signal(signal_start, signal_end, 'A', sum_q_a, current_ex, latest_price))
            ex_dict.update(get_signal(signal_start, signal_end, 'B', sum_q_b, current_ex, latest_price))

        # Skip processed rows
        i = j

    return ex_dict
