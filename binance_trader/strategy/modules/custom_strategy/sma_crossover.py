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
    ContractType,
    Side,
)
from binance_trader.data.modules.data_stream_async import DataStreamAsync
from keys import Keys
from binance_trader.user.modules.process_account_details import ProcessAccountDetails


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
        price_log_loc: str,
        start_time: dt.datetime,
        end_time: dt.datetime,
        limit: int,
        start_data_stream: str,
        end_data_stream: str,
        contract_type: str,
        sma_long: int,
        sma_short: int,
        quantity: float,
    ):
        self = SMACrossover()
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
        self._price_log_loc = price_log_loc

        self._sma_short = sma_short
        self._sma_long = sma_long

        self._start_time = start_time
        self._end_time = end_time
        self._quantity = quantity

        self._limit = limit
        self._start_data_stream = start_data_stream
        self._end_data_stream = end_data_stream
        self._contract_type = contract_type
        return self

    @property
    def price_log_loc(self):
        return self._price_log_loc

    @property
    def sma_short_length(self):
        return self._sma_short

    @property
    def sma_long_length(self):
        return self._sma_long

    @property
    def quantity(self):
        return self._quantity

    async def get_curent_asset_position(self):
        account_details = await self.async_client.futures_account()
        account_details = ProcessAccountDetails(account_details).account_details
        positions = account_details["positions"].loc[self.symbol.upper()]

        if float(positions["notional"]) > 0:
            return "LONG"
        if float(positions["notional"]) < 0:
            return "SHORT"
        return "NO_POSITION"

    async def get_qty_to_trade(self):
        curr_pos = await self.get_curent_asset_position()
        if curr_pos != "NO_POSITION":
            return self.quantity * 2
        return self.quantity

    async def buy(self):
        qty = await self.get_qty_to_trade()
        await self.create_new_order(
            side=Side.Buy, type=FutureOrder.Market, quantity=qty
        )

    async def sell(self):
        qty = await self.get_qty_to_trade()
        await self.create_new_order(
            side=Side.Sell, type=FutureOrder.Market, quantity=qty
        )

    async def stream_candles(self):
        data_stream = await DataStreamAsync.stream(
            db=self.price_log_loc,
            testnet=self.testnet,
            pair=self.symbol,
            contractType=self.contract_type,
            interval=self.interval,
            limit=self.limit,
            start=self.start_data_stream,
            end=self.end_data_stream,
        )

        return data_stream

    async def run_strategy(self):
        data_stream = await self.stream_candles()
        while self.start_time <= self.end_time:
            d = await data_stream.stream_contract()
            asyncio.sleep(100)
            # print(self.start_time, self.end_time)


async def SMAStrategyRun(
    api_key: str,
    api_secret: str,
    testnet: bool,
    interval: str,
    symbol: str,
    order_log_loc: str,
    price_log_loc: str,
    start_time: dt.datetime,
    end_time: dt.datetime,
    limit: int,
    start_data_stream: str,
    end_data_stream: str,
    contract_type: str,
    sma_long: int,
    sma_short: int,
    quantity: float,
):
    sma = await SMACrossover.create(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        interval=interval,
        symbol=symbol,
        order_log_loc=order_log_loc,
        price_log_loc=price_log_loc,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        start_data_stream=start_data_stream,
        end_data_stream=end_data_stream,
        contract_type=contract_type,
        sma_long=sma_long,
        sma_short=sma_short,
        quantity=quantity,
    )

    await sma.run_strategy()
    await sma.async_client.close_connection()


if __name__ == "__main__":
    api_key = Keys.API
    api_secret = Keys.SECRET
    testnet = True
    interval = AsyncClient.KLINE_INTERVAL_1MINUTE
    symbol = "BTCUSDT"
    order_log_loc = (
        "/home/rishabh/projects/binance-trader/binance_trader/data/db/account"
    )
    price_log_loc = "/home/rishabh/projects/binance-trader/binance_trader/data/db/price"
    start_time = dt.datetime.now()
    end_time = dt.datetime(2022, 9, 4, 23, 59, 0)
    start_data_stream = "2 minutes ago UTC"
    end_data_stream = ""
    contract_type = ContractType.Perpetual
    sma_long = 52
    sma_short = 23
    limit = sma_long + 1
    quantity = 0.01

    asyncio.run(
        SMAStrategyRun(
            api_key,
            api_secret,
            testnet,
            interval,
            symbol,
            order_log_loc,
            price_log_loc,
            start_time,
            end_time,
            limit,
            start_data_stream,
            end_data_stream,
            contract_type,
            sma_long,
            sma_short,
            quantity,
        )
    )
