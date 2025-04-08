from settings import *

def real_time_feed(df):
    for i in range(len(df)):
        yield df.iloc[:i+1]  # slice simulates current state of the market up to "now"