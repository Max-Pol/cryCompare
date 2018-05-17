# CryptoScrap

Python scraper to **retrieve all the cryptocurrency historical data** from [CryptoCompare](www.cryptocompare.com).

In short, it **makes several requests** to CryptoCompare [HistoDay](https://www.cryptocompare.com/api/#-api-data-histoday-), [HistoHour](https://www.cryptocompare.com/api/#-api-data-histohour-) and [HistoMinute](https://www.cryptocompare.com/api/#-api-data-histominute-) API endpoints, **aggregates** the resulting data and **stores** it on disk in a `.csv` file. The Scraper will first look for existing files on disk, and retrieve only the missing data.

The files are saved under this format:

- **path** : `<histoType>/<market>.csv` (ex: `hour/BTC-USD.csv`)
- **CSV Header**: `time`, `open`, `high`, `low`, `close`, `volumefrom`, `volumeto`

### Usage
To install the Scraper, just run "`pip install .`" in the **parent** cryptoscrap directory (where the `setup.py` file is located).

Then, the use is pretty straight forward:

```python
from cryptoscrap.scraper import Scraper


s = Scraper("path/where/data/will/be/stored")

# Scrap the historical data of ALL active coins
s.scrap_minute()     # retrieve minute data for ***-BTC (the default market is Bitcoin)
s.scrap_hour("USD")  # retrieve hourly data for ***-USD
s.scrap_day("ETH")   # retrieve daily  data for ***-ETH


# The coins are scrapped by order of their market cap. You can stop
# the scrapping at any time with a KeyboardInterrupt (Ctrl + C).

# If you wish to force the Scraper to rewrite the existing files
# and not append them, just add the argument "update=False".


# Scrap the historical data of a specific market
s.scrap_coin_minute("ETH", "USD")  # retrieve minute data for the market ETH-USD
s.scrap_coin_hour("LTC", "BTC")    # retrieve hourly data for the market LTC-BTC
s.scrap_coin_day("DASH", "BTC")    # retrieve minute data for the market DASH-BTC


# Retrieve the list of all active coins, sorted by marketcap
active_coins = s.get_active_coin_list()

```

Please note that:
- CryptoCompare API allows to retrieve **minute data** only for the past 7 days
- "Active coins" are cryptocurrencies having recent data available on *CoinMarketCap*

### Requirements
- requests
- pandas

### Credits
A special thanks goes to [stefs304](https://github.com/stefs304) and his [cryCompare](https://github.com/stefs304/cryCompare) wrapper I use in this Scraper.
