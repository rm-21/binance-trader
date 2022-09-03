import asyncio
import datetime as dt
import multiprocessing
import os

from binance import AsyncClient, BinanceSocketManager
from binance.enums import *
from binance_trader.data.modules.data_processing import ProcessStream


class BaseDataStream:
    @classmethod
    async def stream_data_task(
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


class DataStream:
    def __init__(
        self,
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
        futures_type: FuturesType = FuturesType.USD_M,
        contract_type: ContractType = ContractType.PERPETUAL,
    ) -> None:
        self.db = db
        self.stream_end = stream_end
        self.symbol = symbol
        self.interval = interval
        self.futures_type = futures_type
        self.contract_type = contract_type

    async def start(self):
        self.loop_task = asyncio.create_task(
            BaseDataStream.stream_data_task(
                db=self.db,
                stream_end=self.stream_end,
                symbol=self.symbol,
                interval=self.interval,
                futures_type=self.futures_type,
                contract_type=self.contract_type,
            )
        )

        await asyncio.wait([self.loop_task])

    async def stop(self):
        self.loop_task.cancel()


class StreamingData:
    @staticmethod
    def contruct_job(
        db: str,
        stream_end: dt.datetime,
        symbol: str,
        interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
        futures_type: FuturesType = FuturesType.USD_M,
        contract_type: ContractType = ContractType.PERPETUAL,
    ):
        job = DataStream(
            db=db,
            stream_end=stream_end,
            symbol=symbol,
            interval=interval,
            futures_type=futures_type,
            contract_type=contract_type,
        )
        return job

    @staticmethod
    def start_stream(stream):
        try:
            asyncio.run(stream.start())
        except KeyboardInterrupt:
            asyncio.run(stream.stop())


if __name__ == "__main__":
    db = "/home/rishabh/projects/binance-trader/binance_trader/data/db"
    stream_end = dt.datetime(2022, 9, 3, 13, 59, 0, 0)

    jobs = [
        StreamingData.contruct_job(
            db=db,
            stream_end=stream_end,
            symbol="BTCUSDT",
            interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
            futures_type=FuturesType.USD_M,
            contract_type=ContractType.PERPETUAL,
        ),
        StreamingData.contruct_job(
            db=db,
            stream_end=stream_end,
            symbol="ETHUSDT",
            interval=AsyncClient.KLINE_INTERVAL_1MINUTE,
            futures_type=FuturesType.USD_M,
            contract_type=ContractType.PERPETUAL,
        ),
    ]

    with multiprocessing.Pool(min(len(jobs), os.cpu_count())) as pool:
        try:
            pool.map(StreamingData.start_stream, jobs)
        except KeyboardInterrupt:
            pool.close()
            pool.join()
