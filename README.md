# Binance Asynchronous Trading
A complete crypto trading implementation in Python for Binance implemented in an asynchronous fashion on Binance Testnet Futures.

## To Get Started:
* Install [poetry](https://python-poetry.org/) as a package manager.
* Clone the repo and run `poetry install` to setup the virtual environment.
* Configure your `keys.py` file. You can get the API keys from the [Testnet](https://testnet.binancefuture.com/en/futures/BTCUSDT) portal.
* Navigate to [`app.py`](https://github.com/rm-21/binance-trader/blob/master/binance_trader/app/app.py) and make changes as you seem fit.
* Currently only implementation for SMA Crossover is added. But you can inherit the [`BaseStrategy.py`](https://github.com/rm-21/binance-trader/blob/master/binance_trader/strategy/modules/base_strategy.py) and write a custom strategy for yourself.

## Breakdown
To understand briefly why everything was done in a certain manner, I'd recommend you to read this [doc](https://docs.google.com/document/d/1yCSbQvVH9AllTitLYur8DmooMPNMf8yXA08vu2ou-dM/edit?usp=sharing).

## At a Glance
* The [DB](https://github.com/rm-21/binance-trader/tree/master/binance_trader/data/db) contains the sample data at the minute interval and the logs for order placed on a single account.
* [`SMACrossover`](https://github.com/rm-21/binance-trader/blob/master/binance_trader/strategy/modules/custom_strategy/sma_crossover.py) is the main class that runs the bot via `SMAStrategyRun` function in `app.py` 