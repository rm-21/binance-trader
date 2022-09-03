import threading
from typing_extensions import Self
import pandas as pd
import asyncio
from binance import AsyncClient, BinanceSocketManager
from keys import Keys


async def futures_account_details(api_key: str, api_secret: str, testnet: bool = True):
    async_client = AsyncClient(api_key=api_key, api_secret=api_secret, testnet=testnet)
    account_details = await async_client.futures_account()
    await async_client.close_connection()
    return account_details


if __name__ == "__main__":
    details = asyncio.run(
        futures_account_details(api_key=Keys.API, api_secret=Keys.SECRET, testnet=True)
    )

    print(details)
