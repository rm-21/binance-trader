import datetime as dt
from typing import Any
import csv
import os.path


class ProcessStream:
    def __init__(self, db: str, row: str) -> None:
        self._db = db
        self._row = row

    def get_conn_url(self) -> str:
        return self._conn_url

    def get_row_data(self) -> dict:
        return self._row

    def write_data(self):
        return self._write_data()

    def _write_data(self):
        pair = self._row["ps"].lower()
        contract = self._row["ct"].lower()
        interval = self._row["k"]["i"]
        stream = f"{pair}_{contract}_{interval}"
        row, is_closed = self._process_data_dict()

        file_name = f"{stream}.csv"
        return self._file_writer(row=row, file_name=file_name)

    def _file_writer(self, row, file_name):
        try:
            file_exists = os.path.isfile(f"{self._db}/{file_name}")
            with open(f"{self._db}/{file_name}", "a") as file:
                writer = csv.DictWriter(
                    file,
                    delimiter=",",
                    lineterminator="\n",
                    fieldnames=list(row.keys()),
                )

                if not file_exists:
                    print(f"Creating file @ {self._db}/{file_name}....")
                    writer.writeheader()

                print(f"{dt.datetime.now()} Writing to File: {file_name}")
                writer.writerow(row)
        except Exception as e:
            print(e)

    def _process_data_dict(self) -> tuple[dict[Any, Any], bool]:
        headers = [
            "sys_time",
            "event_time",
            "symbol",
            "contract_type",
            "k_start_time",
            "k_close_time",
            "interval",
            "open",
            "high",
            "low",
            "close",
            "base_asset_vol",
            "num_trades",
            "quote_asset_vol",
            "taker_buy_base_asset_vol",
            "taker_buy_quote_asset_vol",
            "is_closed",
        ]

        row = [
            dt.datetime.now(),
            self._row["E"],
            self._row["ps"],
            self._row["ct"],
            dt.datetime.strftime(
                dt.datetime.fromtimestamp(self._row["k"]["t"] / 1000.0),
                "%Y-%m-%d %H:%M:%S",
            ),
            dt.datetime.strftime(
                dt.datetime.fromtimestamp(self._row["k"]["T"] / 1000.0),
                "%Y-%m-%d %H:%M:%S",
            ),
            self._row["k"]["i"],
            self._row["k"]["o"],
            self._row["k"]["h"],
            self._row["k"]["l"],
            self._row["k"]["c"],
            self._row["k"]["v"],
            self._row["k"]["n"],
            self._row["k"]["q"],
            self._row["k"]["V"],
            self._row["k"]["Q"],
            self._row["k"]["x"],
        ]
        is_closed = self._row["k"]["x"]

        return (
            dict(zip(headers, row)),
            is_closed,
        )


class ProcessCandle:
    def __init__(
        self, db: str, row: list, symbol: str, contract_type: str, interval: str
    ) -> None:
        self._db = db
        self._row = row
        self.file_name = f"{symbol}_{contract_type}_{interval}.csv"

    @property
    def write_data(self):
        row = self._process_candle()
        return self._file_writer(row=row)

    def _file_writer(self, row):
        try:
            curr_date = dt.datetime.strftime(dt.datetime.now(), "%Y_%M_%d_%H_%M")
            file_exists = os.path.isfile(f"{self._db}/{curr_date}_{self.file_name}")
            with open(f"{self._db}/{curr_date}_{self.file_name}", "a") as file:
                writer = csv.DictWriter(
                    file,
                    delimiter=",",
                    lineterminator="\n",
                    fieldnames=list(row.keys()),
                )

                if not file_exists:
                    print(
                        f"Creating file @ {self._db}/{curr_date}_{self.file_name}...."
                    )
                    writer.writeheader()

                print(
                    f"{dt.datetime.now()} Writing to File: {curr_date}_{self.file_name}"
                )
                writer.writerow(row)
        except Exception as e:
            print(e)

    def _process_candle(self):
        headers = [
            "sys_time",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "close_time",
        ]

        row = [
            dt.datetime.now(),
            dt.datetime.strftime(
                dt.datetime.fromtimestamp(self._row[0] / 1000.0),
                "%Y-%m-%d %H:%M:%S",
            ),
            self._row[1],
            self._row[2],
            self._row[3],
            self._row[4],
            dt.datetime.strftime(
                dt.datetime.fromtimestamp(self._row[6] / 1000.0),
                "%Y-%m-%d %H:%M:%S",
            ),
        ]

        return dict(zip(headers, row))
