import os
import re
import time
from datetime import datetime
import pandas as pd

from .history import histo_day, histo_hour, histo_minute
from .price import coin_list


CSV_HEADER = ['time', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']
HISTO_LIMIT = 2000
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
COINMARKETCAP_TICKER_URL = "https://api.coinmarketcap.com/v1/ticker/?limit=0"
# Note that the CoinMarketCap API v1 will be deprecated on November 30th, 2018
COINMARKETCAP_TO_CRYPTOCOMPARE = {"MIOTA": "IOT",
                                  "NANO": "XRB",
                                  "ETHOS": "BQX"}
CRYPTOCOMPARE_EXPECTED_ERROR = r"(.*)only available for the last 7 days(.*)"


class Scraper():
    """
    Scraper to dump easily the CryptoCompare Histo data (day, hour and minutes)
    into csv files.
    """
    def __init__(self, path_root):
        """
        :param path_root: path where the csv files will be saved
        """
        # Set the storing paths
        self.path_root = path_root
        self.path_day = os.path.join(path_root, "day")
        self.path_hour = os.path.join(path_root, "hour")
        self.path_minute = os.path.join(path_root, "minute")

        # Create missing directory
        for directory in [self.path_day, self.path_hour, self.path_minute]:
            if not os.path.exists(directory):
                os.makedirs(directory)

    # Scraping methods
    def scrap_day(self, to_curr="BTC", update=True):
        """
        Scrap all the daily data of active coins.

        :params update: if set to True, the Scraper will try to append
                        existing data.
        """
        coinlist = [coin for coin in self.get_active_coin_list()
                    if coin != to_curr]
        print("\nScrapping daily data for %d coins..." % len(coinlist))
        for c in coinlist:
            try:
                self.scrap_coin_day(c, to_curr, update=update)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print("ERROR: failed to scrap coin %s: %s" % (c, str(e)))

    def scrap_hour(self, to_curr="BTC", update=True):
        """
        Scrap all the hourly data of active coins.

        :params update: if set to True, the Scraper will try to append
                        existing data.
        """
        coinlist = [coin for coin in self.get_active_coin_list()
                    if coin != to_curr]
        print("\nScrapping hourly data for %d coins..." % len(coinlist))
        for c in coinlist:
            try:
                self.scrap_coin_hour(c, to_curr, update=update)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print("ERROR: failed to scrap coin %s: %s" % (c, str(e)))

    def scrap_minute(self, to_curr="BTC", update=True):
        """
        Scrap all the minute data of active coins.

        :params update: if set to True, the Scraper will try to append
                        existing data.
        """
        coinlist = [coin for coin in self.get_active_coin_list()
                    if coin != to_curr]
        print("\nScrapping minute data for %d coins..." % len(coinlist))
        for c in coinlist:
            try:
                self.scrap_coin_minute(c, to_curr, update=update)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print("ERROR: failed to scrap coin %s: %s" % (c, str(e)))

    # Dumping methods
    def scrap_coin_day(self, from_curr, to_curr="BTC", update=True):
        """
        Dump CryptoCompare HistoDay data into a csv.
        """
        print("Scraping daily data of market %s-%s..." % (from_curr, to_curr))
        filename = from_curr + "-" + to_curr + ".csv"
        csv_path = os.path.join(self.path_day, filename)

        data = histo_day(from_curr, to_curr, all_data=True)
        df = pd.DataFrame(data, columns=CSV_HEADER)
        df["time"] = pd.to_datetime(df["time"], unit='s')
        # TOFIX: case when volumeto is 0 (last row for instance)

        df.to_csv(csv_path, index=False)

    def scrap_coin_hour(self, from_curr, to_curr="BTC",
                        update=True, verbose=False):
        """
        Dump CryptoCompare HistoHour data into a csv.

        If update = True, the scraper will first try to load
        existing data in the root path, and retrieve only the missing
        data from CryptoCompare.
        """
        print("Scraping hourly data of market %s-%s..." % (from_curr, to_curr))
        filename = from_curr + "-" + to_curr + ".csv"
        csv_path = os.path.join(self.path_hour, filename)

        # If csv already exist, load it and retrieve last timestamp
        df_existing = None
        ts_end = 0
        if update and os.path.isfile(csv_path):
            df_existing = pd.read_csv(csv_path)
            ts_end = str_to_ts(df_existing.iloc[-1, 0], DATE_FORMAT)

        # Retrieve first chunk of data from CryptoCompare
        ts = int(time.time())
        data = histo_hour(from_curr, to_curr, limit=HISTO_LIMIT, to_ts=ts)
        df = pd.DataFrame(data, columns=CSV_HEADER)

        # Retrieve data from CryptoCompare until enough data have been fetched
        # i.e. no more data is available (high price = 0), or remaining data
        # is already in the existing csv
        while df.loc[0, "high"] > 0 and df.loc[0, "time"] > ts_end:
            ts = int(df.head(1)["time"])

            if verbose:
                print("[Cryptocompare] API Call: %s-%s, to date %s"
                      % (from_curr, to_curr, ts_to_str(ts)))
            data = histo_hour(from_curr, to_curr,
                              limit=HISTO_LIMIT, to_ts=ts - 1)
            df2 = pd.DataFrame(data, columns=CSV_HEADER)
            df = pd.concat([df2, df], axis=0, ignore_index=True)

        # Format data: clean zeros and convert to datetime
        for idx, row in df.iterrows():
            if not row[2] == row[3] == 0:
                df = df.drop(df.index[:idx])
                break
        df["time"] = pd.to_datetime(df["time"], unit='s')

        # Merge with existing csv data
        if df_existing is not None:
            for idx, row in df.iterrows():
                if str(row[0]) == str(df_existing.iloc[-1, 0]):
                    df = df.drop(df.index[:idx + 1])
                    df = pd.concat([df_existing, df],
                                   axis=0, ignore_index=True)
                    break

        df.to_csv(csv_path, index=False)

    def scrap_coin_minute(self, from_curr, to_curr="BTC",
                          update=True, verbose=False):
        """
        Dump CryptoCompare HistoMinute data into a csv.

        If update = True, the scraper will first try to load
        existing data in the root path, and retrieve only the missing
        data from CryptoCompare.
        """
        print("Scraping minute data of market %s-%s..." % (from_curr, to_curr))
        filename = from_curr + "-" + to_curr + ".csv"
        csv_path = os.path.join(self.path_minute, filename)

        # If csv already exist, load it and retrieve last timestamp
        df_existing = None
        ts_end = 0
        if update and os.path.isfile(csv_path):
            df_existing = pd.read_csv(csv_path)
            ts_end = str_to_ts(df_existing.iloc[-1, 0], DATE_FORMAT)

        # Retrieve first chunk of data from CryptoCompare
        ts = int(time.time())
        data = histo_minute(from_curr, to_curr, limit=HISTO_LIMIT, to_ts=ts)
        df = pd.DataFrame(data, columns=CSV_HEADER)

        # Retrieve data from CryptoCompare until enough data have been fetched
        # i.e. no more data is available (high price = 0), or remaining data
        # is already in the existing csv
        while df.loc[0, "high"] > 0 and df.loc[0, "time"] > ts_end:
            ts = int(df.head(1)["time"])

            if verbose:
                print("[Cryptocompare] API Call: %s-%s, to date %s"
                      % (from_curr, to_curr, ts_to_str(ts)))
            try:
                data = histo_minute(from_curr, to_curr,
                                    limit=HISTO_LIMIT, to_ts=ts - 1)
            except ValueError as e:
                if re.match(CRYPTOCOMPARE_EXPECTED_ERROR, str(e)):
                    break
                raise e

            df2 = pd.DataFrame(data, columns=CSV_HEADER)
            df = pd.concat([df2, df], axis=0, ignore_index=True)

        # Format data: clean zeros and convert to datetime
        for idx, row in df.iterrows():
            if not row[2] == row[3] == 0:
                df = df.drop(df.index[:idx])
                break
        df["time"] = pd.to_datetime(df["time"], unit='s')

        # Merge with existing csv data
        if df_existing is not None:
            for idx, row in df.iterrows():
                if str(row[0]) == str(df_existing.iloc[-1, 0]):
                    df = df.drop(df.index[:idx + 1])
                    df = pd.concat([df_existing, df],
                                   axis=0, ignore_index=True)
                    break

        df.to_csv(csv_path, index=False)

    def get_active_coin_list(self):
        """
        Return a list of active coins, sorted by market cap.
        To be eligible, the coin need to be referenced on both
        Cryptocompare and Coinmarketcap.

        :return: list
        """

        # Retrieve active coin list from CoinMarketCap,
        # sorted by marketcap
        df = pd.read_json(COINMARKETCAP_TICKER_URL)
        cmc_list = df["symbol"].tolist()

        # Retrieve coin list from CryptoCompare
        cc_list = list(coin_list()["Data"].keys())

        # Solve different naming issues
        # (for instance: CryptoCompare IOT = CoinMarketCap MIOTA)
        for idx, crypto in enumerate(cmc_list):
            if crypto in list(COINMARKETCAP_TO_CRYPTOCOMPARE.keys()):
                cmc_list[idx] = COINMARKETCAP_TO_CRYPTOCOMPARE[crypto]

        # Intersection of CoinMarketCap and CryptoCompare list
        common_list = [k for k in cmc_list if k in cc_list]

        print("%d available coins on CoinMarketCap" % (len(cmc_list)))
        print("%d available coins on CryptoCompare" % (len(cc_list)))
        print("%d available coins in common" % len(common_list))

        return common_list


# Utils
def ts_to_str(ts):
    """Convert timestamp to datetime string"""
    return datetime.fromtimestamp(int(ts)).strftime(DATE_FORMAT)


def str_to_ts(date_str, str_format=DATE_FORMAT):
    """Convert datetime string to timestamp"""
    dt = datetime.strptime(date_str, str_format)
    return time.mktime(dt.timetuple())
