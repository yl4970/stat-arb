class TradeEngine:
    def __init__(self, transaction_fee=0.50):
        self.position = 0
        self.entry_price = None
        self.realized_pnl = 0
        self.transaction_fee = transaction_fee

    def enter_trade(self, price, direction):
        self.position = 1 if direction == 'long' else -1
        self.entry_price = price

    def exit_trade(self, price):
        if self.position != 0:
            pnl = (price - self.entry_price) * self.position - self.transaction_fee
            self.realized_pnl += pnl
            self.position = 0
            self.entry_price = None

    def current_pnl(self, current_price):
        if self.position == 0:
            return self.realized_pnl
        unrealized = (current_price - self.entry_price) * self.position
        return self.realized_pnl + unrealized