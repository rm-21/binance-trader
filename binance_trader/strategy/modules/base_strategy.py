import asyncio
import csv
import datetime as dt
import decimal
import os

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
        cls,
        api_key: str,
        api_secret: str,
        testnet: bool,
        interval: str,
        symbol: str,
        order_log_loc: str,
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
        return self

    @property
    def symbol(self):
        return self._symbol

    @property
    def interval(self):
        return self._interval

    @property
    def symbol_info(self):
        return self._symbol_info

    @property
    def filters(self):
        return self._filters

    @property
    def order_log_location(self):
        return self._order_log_location

    async def symbol_info(self):
        symbol_info = await self.async_client.get_symbol_info(symbol=self.symbol)
        filters = pd.DataFrame(symbol_info["filters"]).set_index(
            "filterType", drop=True
        )
        return (symbol_info, filters)

    async def _min_qty(self):
        min_notional_val = float(self.filters.loc["MIN_NOTIONAL"]["minNotional"])
        avg_price_dict = await self.async_client.get_avg_price(symbol=self.symbol)
        avg_price = float(avg_price_dict["price"])
        return (min_notional_val / avg_price, avg_price)

    async def _process_qty(self, qty):
        min_qty, avg_price = await self._min_qty()
        round_to_digits = float(self.filters.loc["PRICE_FILTER"]["tickSize"])
        final_qty = round_step_size(qty, round_to_digits)

        if final_qty < min_qty:
            raise ValueError(f"{final_qty} is less than than {min_qty=}")
        return final_qty

    async def create_new_order(self, side: Side, type: FutureOrder, quantity: decimal):
        qty = await self._process_qty(quantity)

        try:
            order_result = await self.async_client.futures_create_order(
                symbol=self.symbol, side=side, type=type, quantity=qty
            )
            self._file_writer(order_result, "order_log.csv")

        except BinanceAPIException as e:
            print(e)
            await self.async_client.close_connection()
            return None

        return qty

    def _file_writer(self, row, file_name):
        try:
            file_exists = os.path.isfile(f"{self.order_log_location}/{file_name}")
            with open(f"{self.order_log_location}/{file_name}", "a") as file:
                writer = csv.DictWriter(
                    file,
                    delimiter=",",
                    lineterminator="\n",
                    fieldnames=list(row.keys()),
                )

                if not file_exists:
                    print(f"Creating file @ {self.order_log_location}/{file_name}....")
                    writer.writeheader()

                print(f"{dt.datetime.now()} Writing to File: {file_name}")
                writer.writerow(row)
        except Exception as e:
            print(e)


async def BaseStrategyRun(
    api_key: str, api_secret: str, testnet: bool, interval: str, symbol: str
):
    base_strat_obj = await BaseStrategy.create(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        interval=interval,
        symbol=symbol,
        order_log_loc="/home/rishabh/projects/binance-trader/binance_trader/data/db/account",
    )

    await base_strat_obj.create_new_order(Side.Buy, FutureOrder.Market, 0.05)
    await asyncio.sleep(5)
    await base_strat_obj.create_new_order(Side.Sell, FutureOrder.Market, 0.05 * 2)
    await asyncio.sleep(5)
    await base_strat_obj.create_new_order(Side.Buy, FutureOrder.Market, 0.05)

    await base_strat_obj.async_client.close_connection()


async def run_parallel(jobs):
    await asyncio.gather(*jobs)


if __name__ == "__main__":
    jobs = [
        BaseStrategyRun(
            api_key=Keys.API,
            api_secret=Keys.SECRET,
            testnet=True,
            interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
            symbol="BTCUSDT",
        ),
        BaseStrategyRun(
            api_key=Keys.API,
            api_secret=Keys.SECRET,
            testnet=True,
            interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
            symbol="ETHUSDT",
        ),
        BaseStrategyRun(
            api_key=Keys.API,
            api_secret=Keys.SECRET,
            testnet=True,
            interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
            symbol="ETHUSDT",
        ),
    ]

    asyncio.run(run_parallel(jobs))
