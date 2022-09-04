import asyncio
import datetime as dt

import pandas as pd
from binance import AsyncClient
from binance.enums import *
from binance.exceptions import BinanceAPIException
from binance.helpers import round_step_size
from binance_trader.strategy.modules.base_strategy import BaseStrategy
from binance_trader.strategy.modules.models.models import (
    FutureOrder,
    OrderFillType,
    OrderResponse,
    OrderStatus,
    Side,
)
from keys import Keys


class SMACrossover(BaseStrategy):
    @classmethod
    async def create(
        cls,
        api_key: str,
        api_secret: str,
        testnet: bool,
        interval: str,
        symbol: str,
        order_log_loc: str,
        sma_long: int,
        sma_short: int,
    ):
        self = BaseStrategy()
        self._interval = interval
        self._symbol = symbol
        self._api_key = api_key
        self._api_secret = (api_secret,)
        self._testnet = testnet
        self.async_client = await AsyncClient().create(
            api_key=api_key, api_secret=api_secret, testnet=testnet
        )
        self._symbol_info, self._filters = await self.symbol_info()
        self._order_log_location = order_log_loc

        self._sma_short = sma_short
        self._smal_long = sma_long
        return self

    @property
    def sma_short_length(self):
        return self._sma_short

    @property
    def sma_long_length(self):
        return self._sma_long


async def SMAStrategyRun(
    api_key: str, api_secret: str, testnet: bool, interval: str, symbol: str
):
    base_strat_obj = await SMACrossover.create(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        interval=interval,
        symbol=symbol,
        order_log_loc="/home/rishabh/projects/binance-trader/binance_trader/data/db/account",
        sma_long=52,
        sma_short=23,
    )

    await base_strat_obj.create_new_order(Side.Buy, FutureOrder.Market, 0.05)
    await asyncio.sleep(5)
    await base_strat_obj.create_new_order(Side.Sell, FutureOrder.Market, 0.05)
    await asyncio.sleep(5)
    await base_strat_obj.create_new_order(Side.Buy, FutureOrder.Market, 0.05)

    await base_strat_obj.async_client.close_connection()


if __name__ == "__main__":
    asyncio.run(
        SMAStrategyRun(
            api_key=Keys.API,
            api_secret=Keys.SECRET,
            testnet=True,
            interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
            symbol="BTCUSDT",
        )
    )
