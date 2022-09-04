import asyncio
import datetime as dt
import multiprocessing
import os
import time

from binance import AsyncClient, BinanceSocketManager
from binance.enums import *
from binance_trader.data.modules.data_processing import ProcessStream


class BaseDataStream:
    @classmethod
    async def futures_socket(
        cls,
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
        futures_type: FuturesType = FuturesType.USD_M,
        contract_type: ContractType = ContractType.PERPETUAL,
    ) -> None:
        client = await AsyncClient.create()
        bm = BinanceSocketManager(client)
        data_stream = bm.kline_futures_socket(
            symbol=symbol,
            interval=interval,
            futures_type=futures_type,
            contract_type=contract_type,
        )
        while dt.datetime.now() < stream_end:
            async with data_stream as stream:
                res = await stream.recv()
                ProcessStream(db=db, row=res).write_data()
        await client.close_connection()

    @classmethod
    async def futures_historical(
        cls,
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        start_str: str,
        end_str: str,
        limit: int,
        interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
    ) -> None:
        client = await AsyncClient.create()
        data_stream = client.futures_historical_klines(
            symbol=symbol,
            start_str=start_str,
            end_str=end_str,
            limit=limit,
            interval=interval,
        )
        while dt.datetime.now() < stream_end:
            while True:
                time.sleep(60)
                res = await data_stream
                print(res)
        # ProcessStream(db=db, row=res).write_data()
        await client.close_connection()


class DataStream:
    def __init__(
        self,
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        interval: str = AsyncClient.KLINE_INTERVAL_1MINUTE,
    ) -> None:
        self.db = db
        self.stream_end = stream_end
        self.symbol = symbol
        self.interval = interval

    async def start(self):
        pass

    async def stop(self):
        self.loop_task.cancel()


class SocketStream(DataStream):
    def __init__(
        self,
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        interval: str = AsyncClient.KLINE_INTERVAL_1MINUTE,
        futures_type: FuturesType = FuturesType.USD_M,
        contract_type: ContractType = ContractType.PERPETUAL,
    ) -> None:
        super().__init__(db, stream_end, symbol, interval)
        self.futures_type = futures_type
        self.contract_type = contract_type

    def loop_task(self):
        return BaseDataStream.futures_socket(
            db=self.db,
            stream_end=self.stream_end,
            symbol=self.symbol,
            interval=self.interval,
            futures_type=self.futures_type,
            contract_type=self.contract_type,
        )

    async def start(self):
        self.loop_task = self.loop_task()
        await asyncio.wait({self.loop_task})


class HistoricalStream(DataStream):
    def __init__(
        self,
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        start_str: str,
        end_str: str,
        limit: int,
        interval: str = AsyncClient.KLINE_INTERVAL_1MINUTE,
    ) -> None:
        super().__init__(db, stream_end, symbol, interval)
        self.start_str = start_str
        self.end_str = end_str
        self.limit = limit

    def loop_task(self):
        return BaseDataStream.futures_historical(
            db=self.db,
            stream_end=self.stream_end,
            symbol=self.symbol,
            interval=self.interval,
            start_str=self.start_str,
            end_str=self.end_str,
            limit=self.limit,
        )

    async def start(self):
        self.loop_task = self.loop_task()
        done, pending = await asyncio.wait({self.loop_task})


class DataJob:
    @staticmethod
    def SocketStreamJob(
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        interval: str = AsyncClient.KLINE_INTERVAL_1MINUTE,
        futures_type: FuturesType = FuturesType.USD_M,
        contract_type: ContractType = ContractType.PERPETUAL,
    ):
        return SocketStream(
            db=db,
            stream_end=stream_end,
            symbol=symbol,
            interval=interval,
            futures_type=futures_type,
            contract_type=contract_type,
        )

    @staticmethod
    def HistoricalStreamJob(
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        start_str: str,
        end_str: str,
        limit: int,
        interval: str = AsyncClient.KLINE_INTERVAL_1MINUTE,
    ):
        return HistoricalStream(
            db=db,
            stream_end=stream_end,
            symbol=symbol,
            start_str=start_str,
            end_str=end_str,
            limit=limit,
            interval=interval,
        )

    @staticmethod
    def start_stream(stream):
        try:
            asyncio.run(stream.start())
        except KeyboardInterrupt:
            asyncio.run(stream.stop())


if __name__ == "__main__":
    db = "/home/rishabh/projects/binance-trader/binance_trader/data/db"
    stream_end = dt.datetime(2022, 9, 3, 16, 59, 0, 0)
    symbol = "BTCUSDT"
    interval = AsyncClient.KLINE_INTERVAL_1MINUTE

    jobs = [
        DataJob.SocketStreamJob(
            db=db,
            stream_end=stream_end,
            interval=interval,
            symbol="BTCUSDT",
            futures_type=FuturesType.USD_M,
            contract_type=ContractType.PERPETUAL,
        ),
        DataJob.SocketStreamJob(
            db=db,
            stream_end=stream_end,
            interval=interval,
            symbol="ETHUSDT",
            futures_type=FuturesType.USD_M,
            contract_type=ContractType.PERPETUAL,
        ),
        # DataJob.HistoricalStreamJob(
        #     db=db,
        #     stream_end=stream_end,
        #     interval=interval,
        #     symbol=symbol,
        #     start_str="Sept 3, 2022",
        #     end_str="Sept 4, 2022",
        #     limit=50,
        # ),
    ]

    with multiprocessing.Pool(min(len(jobs), os.cpu_count())) as pool:
        try:
            pool.map(DataJob.start_stream, jobs)
        except KeyboardInterrupt:
            pool.close()
            pool.join()
