import asyncio
from binance import AsyncClient
from binance.enums import *
import datetime as dt
from binance_trader.data.modules.data_processing import ProcessCandle


class ContractType:
    Perpetual = "PERPETUAL"
    CurrentQtr = "CURRENT_QUARTER"
    NextQtr = "NEXT_QUARTER"


class DataStreamAsync:
    @classmethod
    async def stream(
        cls,
        db: str,
        testnet: bool,
        pair: str,
        contractType: ContractType,
        interval: str,
        limit: int,
        start: dt.datetime,
    ):
        self = DataStreamAsync()
        self._db = db
        self._pair = pair
        self._contract_type = contractType
        self._interval = interval
        self._limit = limit
        self._start = start
        self._async_client = await AsyncClient.create(testnet=testnet)
        return self

    @property
    def db(self):
        return self._db

    @property
    def async_client(self):
        return self._async_client

    @property
    def pair(self):
        return self._pair

    @property
    def contract_type(self):
        return self._contract_type

    @property
    def interval(self):
        return self._interval

    @property
    def limit(self):
        return self._limit

    @property
    def start(self):
        return self._start

    async def stream_contract(self):
        data = await self.async_client.futures_continous_klines(
            pair=self.pair,
            contractType=self.contract_type,
            interval=self.interval,
            limit=self.limit,
        )

        for row in data:
            ProcessCandle(
                db=self.db,
                row=row,
                symbol=self.pair,
                contract_type=self.contract_type,
                interval=self.interval,
            ).write_data
        return data

    async def stream_live(self):
        ...


async def stream_candles(
    db: str,
    testnet: bool,
    pair: str,
    contractType: ContractType,
    interval: str,
    limit: int,
    async_client=None,
):
    stream_obj = await DataStreamAsync.stream(
        db=db,
        testnet=testnet,
        pair=pair,
        contractType=contractType,
        interval=interval,
        limit=limit,
        async_client=async_client,
    )

    data = await stream_obj.stream_contract()
    await stream_obj.async_client.close_connection()
    return data


async def run_parallely(jobs):
    await asyncio.gather(*jobs)


if __name__ == "__main__":
    db = "/home/rishabh/projects/binance-trader/binance_trader/data/db/price"
    testnet = True
    contractType = ContractType.Perpetual
    interval = AsyncClient.KLINE_INTERVAL_1MINUTE
    limit = 100

    jobs = [
        stream_candles(
            db=db,
            testnet=testnet,
            pair="BTCUSDT",
            contractType=contractType,
            interval=interval,
            limit=limit,
        ),
        stream_candles(
            db=db,
            testnet=testnet,
            pair="ETHUSDT",
            contractType=contractType,
            interval=interval,
            limit=limit,
        ),
    ]

    asyncio.run(run_parallely(jobs=jobs))
