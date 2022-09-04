import asyncio
import decimal
import json
from pprint import pprint

import pandas as pd
from binance import AsyncClient
from binance.enums import *
from binance.exceptions import BinanceAPIException
from binance.helpers import round_step_size
from binance_trader.strategy.modules.models.models import (
    FutureOrder,
    OrderFillType,
    OrderResponse,
    OrderStatus,
    Side,
)
from keys import Keys


class BaseStrategy:
    @classmethod
    async def create(
        cls, api_key: str, api_secret: str, testnet: bool, interval: str, symbol: str
    ):
        self = BaseStrategy()
        self._interval = interval
        self._symbol = symbol
        self.async_client = await AsyncClient().create(
            api_key=api_key, api_secret=api_secret, testnet=testnet
        )
        self.symbol_info, self.filters = await self.symbol_info()
        return self

    @property
    def symbol(self):
        return self._symbol

    @property
    def interval(self):
        return self._interval

    async def symbol_info(self):
        symbol_info = await self.async_client.get_symbol_info(symbol=self.symbol)
        filters = pd.DataFrame(symbol_info["filters"]).set_index(
            "filterType", drop=True
        )
        return (symbol_info, filters)

    async def min_qty(self):
        min_notional_val = float(self.filters.loc["MIN_NOTIONAL"]["minNotional"])
        avg_price_dict = await self.async_client.get_avg_price(symbol=self.symbol)
        avg_price = float(avg_price_dict["price"])
        return min_notional_val / avg_price

    async def process_qty(self, qty):
        min_qty = await self.min_qty()
        round_to_digits = float(self.filters.loc["PRICE_FILTER"]["tickSize"])
        final_qty = round_step_size(qty, round_to_digits)
        if final_qty < min_qty:
            raise ValueError(f"{final_qty} is less than than {min_qty=}")
        return final_qty

    async def create_new_order(self, side: Side, type: FutureOrder, quantity: decimal):
        qty = await self.process_qty(quantity)
        try:
            order_result = await self.async_client.futures_create_order(
                symbol=self.symbol, side=side, type=type, quantity=qty
            )
        except BinanceAPIException as e:
            print(e)
        else:
            print(json.dumps(order_result))

        return qty


async def BaseStrategyRun(
    api_key: str, api_secret: str, testnet: bool, interval: str, symbol: str
):
    base_strat_obj = await BaseStrategy.create(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        interval=interval,
        symbol=symbol,
    )

    await base_strat_obj.create_new_order(Side.Sell, FutureOrder.Market, 0.01)
    await base_strat_obj.async_client.close_connection()


if __name__ == "__main__":
    asyncio.run(
        BaseStrategyRun(
            api_key=Keys.API,
            api_secret=Keys.SECRET,
            testnet=True,
            interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
            symbol="BTCUSDT",
        )
    )

    # asyncio.run(base_strat.close_connection())
    # asyncio.run(base_strat.start_connection())
    # asyncio.run(base_strat.async_client.futures_account())
    # print(base_strat.interval)
