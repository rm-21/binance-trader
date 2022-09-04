import asyncio
import datetime as dt

from binance import AsyncClient
from binance_trader.strategy.modules.custom_strategy.sma_crossover import SMAStrategyRun
from binance_trader.strategy.modules.models.models import ContractType
from keys import Keys


async def run_parallel(bots):
    await asyncio.gather(*bots)


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
    end_time = dt.datetime(2022, 9, 5, 23, 59, 0)
    start_data_stream = "2 minutes ago UTC"
    end_data_stream = ""
    contract_type = ContractType.Perpetual
    sma_long = 52
    sma_short = 23
    limit = sma_long + 1

    bot1 = SMAStrategyRun(
        api_key,
        api_secret,
        testnet,
        interval,
        "BTCUSDT",
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
        0.01,
    )

    bot2 = SMAStrategyRun(
        api_key,
        api_secret,
        testnet,
        interval,
        "ETHUSDT",
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
        0.01,
    )

    bot3 = SMAStrategyRun(
        api_key,
        api_secret,
        testnet,
        interval,
        "XRPUSDT",
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
        40,
    )

    bots = [bot1, bot2, bot3]
    asyncio.run(run_parallel(bots))
