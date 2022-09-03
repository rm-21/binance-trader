import asyncio
from binance import AsyncClient
from keys import Keys
from binance_trader.user.modules.process_account_details import ProcessAccountDetails


async def futures_account_details(api_key: str, api_secret: str, testnet: bool = True):
    async_client = AsyncClient(api_key=api_key, api_secret=api_secret, testnet=testnet)
    account_details = await async_client.futures_account()
    await async_client.close_connection()
    account_details = ProcessAccountDetails(account_details).account_details
    return account_details


if __name__ == "__main__":
    details = asyncio.run(
        futures_account_details(api_key=Keys.API, api_secret=Keys.SECRET, testnet=True)
    )

    print(details)
