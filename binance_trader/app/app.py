import asyncio
import datetime as dt
import multiprocessing
import os
import pprint

import pandas as pd
from binance import AsyncClient, BinanceSocketManager, Client
from binance.enums import *
from binance_trader.data.modules.data_stream import DataJob
from keys import Keys


def run_data_stream_job():
    db = "/home/rishabh/projects/binance-trader/binance_trader/data/db/price"
    stream_end = dt.datetime(2022, 9, 3, 19, 59, 0, 0)
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
    ]

    with multiprocessing.Pool(min(len(jobs), os.cpu_count())) as pool:
        try:
            pool.map(DataJob.start_stream, jobs)
        except KeyboardInterrupt:
            pool.close()
            pool.join()


async def main():
    client = AsyncClient(api_key=Keys.API, api_secret=Keys.SECRET, testnet=True)
    resp = await client.futures_account()
    pprint.pprint(resp.get("totalWalletBalance"))
    pprint.pprint(pd.DataFrame(resp.get("positions")))
    await client.close_connection()


if __name__ == "__main__":
    client = Client(api_key=Keys.API, api_secret=Keys.SECRET, testnet=True)
    # pprint.pprint(client.futures_account().keys())

    # pprint.pprint(client.futures_account().get("totalWalletBalance"))
    # pprint.pprint(client.futures_account().get("canTrade"))
    # pprint.pprint(client.futures_account().get("totalCrossWalletBalance"))
    # pprint.pprint(client.futures_account().get("availableBalance"))
    # pprint.pprint(client.futures_account().get("maxWithdrawAmount"))
    # pprint.pprint(client.futures_account().get("assets"))
    # pprint.pprint(client.futures_account().get("positions"))

    asyncio.run(main())
