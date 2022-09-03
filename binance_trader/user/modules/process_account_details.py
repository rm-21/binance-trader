import pandas as pd


class ProcessAccountDetails:
    def __init__(self, resp: dict) -> None:
        self._resp = resp
        self.details = {}

    @property
    def raw_resp(self):
        return self._resp

    @property
    def account_details(self) -> dict:
        self.details["can_trade"] = self._resp["canTrade"]
        self.details["balance"] = self._resp["totalWalletBalance"]
        self.details["assets"] = pd.DataFrame(self._resp["assets"]).set_index(
            "asset", drop=True
        )
        self.details["positions"] = pd.DataFrame(self._resp["positions"]).set_index(
            "symbol", drop=True
        )
        return self.details


if __name__ == "__main__":
    import asyncio
    from pprint import pprint
    from account_details import futures_account_details
    from keys import Keys

    details = asyncio.run(
        futures_account_details(api_key=Keys.API, api_secret=Keys.SECRET, testnet=True)
    )

    x = ProcessAccountDetails(details).account_details
    # print(x.keys())
    # print(x.get("can_trade"), x.get("balance"))

    # pprint(x.get("assets"))
    df = x.get("positions")
    pprint(df.loc["BTCUSDT"])
