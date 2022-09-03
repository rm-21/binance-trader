import datetime as dt
from typing import Any
import csv
import os.path


class ProcessStream:
    def __init__(self, db: str, row: str, interval: bool) -> None:
        self._db = db
        self._row = row
        self._interval = interval

    def get_conn_url(self) -> str:
        return self._conn_url

    def get_row_data(self) -> dict:
        return self._row

    def get_stream_status(self) -> bool:
        return self._interval

    def write_data(self):
        return self._write_data()

    def _write_data(self):
        stream = self._row["stream"]
        row, is_closed = self._process_data_dict()

        if self._interval:
            file_name = f"{stream}_i.csv"

            if is_closed:
                return self._file_writer(row=row, file_name=file_name)
        else:
            file_name = f"{stream}_c.csv"
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
            self._row["data"]["E"],
            self._row["data"]["ps"],
            self._row["data"]["ct"],
            dt.datetime.strftime(
                dt.datetime.fromtimestamp(self._row["data"]["k"]["t"] / 1000.0),
                "%Y-%m-%d %H:%M:%S",
            ),
            dt.datetime.strftime(
                dt.datetime.fromtimestamp(self._row["data"]["k"]["T"] / 1000.0),
                "%Y-%m-%d %H:%M:%S",
            ),
            self._row["data"]["k"]["i"],
            self._row["data"]["k"]["o"],
            self._row["data"]["k"]["h"],
            self._row["data"]["k"]["l"],
            self._row["data"]["k"]["c"],
            self._row["data"]["k"]["v"],
            self._row["data"]["k"]["n"],
            self._row["data"]["k"]["q"],
            self._row["data"]["k"]["V"],
            self._row["data"]["k"]["Q"],
            self._row["data"]["k"]["x"],
        ]
        is_closed = self._row["data"]["k"]["x"]

        return (
            dict(zip(headers, row)),
            is_closed,
        )
