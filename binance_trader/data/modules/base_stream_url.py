from binance import Client
import pandas as pd


class BaseDataStreamURL:
    def __init__(
        self,
        pair: str,
        interval: str = "1m",
        contract_type: str = "perpetual",
        testnet: bool = True,
    ) -> None:

        self._client = Client(testnet=testnet)

        self._pair = self._validate_pair(pair)
        self._interval = self._validate_interval(interval)
        self._contract_type = self._validate_contract_type(contract_type)
        self._socket_stream = self._socket_stream()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} [ {self.pair} | {self.contract_type} | {self._interval} | {self.socket_stream} ]"

    @property
    def socket_stream(self: str) -> str:
        return self._socket_stream

    @property
    def pair(self) -> str:
        return self._pair

    @property
    def contract_type(self) -> str:
        return self._contract_type

    @property
    def binance_client(self):
        return self._client

    def _socket_stream(self: str) -> str:
        return f"{self._pair}_{self._contract_type}@continuousKline_{self._interval}"

    def _validate_pair(self, pair: str) -> str:
        valid_pairs = pd.DataFrame(self.binance_client.get_all_tickers())[
            "symbol"
        ].to_list()
        if pair not in valid_pairs:
            raise ValueError(
                f"{pair} is not a valid ticker. Select from {valid_pairs}."
            )
        return pair.lower()

    def _validate_interval(self, interval: str) -> str:
        valid_intervals = [
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M",
        ]
        if interval not in valid_intervals:
            raise ValueError(
                f"{interval} is not a valid ticker. Select from {valid_intervals}."
            )
        return interval.lower()

    def _validate_contract_type(self, contract_type: str) -> str:
        valid_contract_types = ["perpetual", "current_quarter", "next_quarter"]
        if contract_type not in valid_contract_types:
            raise ValueError(
                f"{contract_type} is not a valid ticker. Select from {valid_contract_types}."
            )
        return contract_type.lower()
