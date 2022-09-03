class BaseStrategy:
    def __init__(self, interval, symbol) -> None:
        self._interval = interval
        self._symbol = symbol

    @property
    def symbol(self):
        return self._symbol

    @property
    def interval(self):
        return self._interval

    def buy_signal(self):
        pass

    def sell_signal(self):
        pass
