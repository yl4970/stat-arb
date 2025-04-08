from signal_processing import load_all as signal
from util import *

# extract_all_tar(TAR_FILE_PATH, GZ_DIR)
data = extract_all_gz(GZ_DIR)
signal_dict = signal(data, threshold=50, latency=10, transaction_fee=50)

print(signal_dict)

engine = TradeEngine(transaction_fee=0.50)
latest_prices = {}

for current_df in real_time_feed(df):
    current_row = current_df.iloc[-1]
    timestamp = current_row.name

    # Update latest prices from current_row
    # Detect opportunity: if spread > threshold and quantity > threshold
    # Use a simplified condition for now
    bid = current_row[('Price', 'B')]
    ask = current_row[('Price', 'A')]

    # Example strategy: market making or latency arb
    if ask < some_other_exchange_bid:
        # Opportunity to buy on A, sell on B
        engine.enter_trade(price=ask, direction='long')
    elif bid > some_other_exchange_ask:
        # Opportunity to sell on A, buy on B
        engine.enter_trade(price=bid, direction='short')

    # Add exit condition
    # (e.g., price reverts, or fixed number of rows later)
    # Then:
    # engine.exit_trade(current_price)

    print(f"[{timestamp}] Current PnL: {engine.current_pnl(current_price)}")