import os
import re
import time
import sys
import socket
from datetime import datetime
import pandas as pd
import logging

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
CRYPTOCOMPARE_NO_DATA_ERROR = r"Cryptocompare API Error: There is no data for the symbol(.*)"
REMOTE_SERVER = "www.google.com"  # to check internet connection
INTERNET_CHECK_RATE = 30 * 60  # 30 minutes


class Scraper():
    """
    Scraper to dump easily the CryptoCompare Histo data (day, hour and minutes)
    into csv files.
    """
    def __init__(self, path_root, logger=None):
        """
        :param path_root: path where the csv files will be saved
        """
        # Set the storing paths
        self.path_root = path_root
        self.path_day = os.path.join(path_root, "day")
        self.path_hour = os.path.join(path_root, "hour")
        self.path_minute = os.path.join(path_root, "minute")
        self.path_coin_ignore = os.path.join(path_root, "coin_ignore_list.csv")

        # Create a stdout logger if None
        if logger is None:
            log = logging.getLogger()
            log.setLevel(logging.INFO)

            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - '
                                          '%(message)s')
            handler.setFormatter(formatter)
            log.addHandler(handler)
            self.log = log
        else:
            self.log = logger

        # Create missing directory
        for directory in [self.path_day, self.path_hour, self.path_minute]:
            if not os.path.exists(directory):
                os.makedirs(directory)

    # Scrap all method
    def scrap(self, rate, to_curr="BTC", update=True, verbose=1):
        """
        Scrap all the data of active coins.

        :param rate: minute/hour/day
        :param update: if set to True, the Scraper will try to append
                        existing data.
        """
        scrap_coin_func = {
            "minute": self.scrap_coin_minute,
            "hour": self.scrap_coin_hour,
            "day": self.scrap_coin_day
        }

        coinlist = [coin for coin in self.get_active_coin_list(verbose=verbose)
                    if coin != to_curr]
        self.log.info("Scrapping %s %s data for %d coins..."
                      % (to_curr, rate, len(coinlist)))
        success = []
        for c in coinlist:
            try:
                scrap_coin_func[rate](c, to_curr, update=update,
                                      verbose=verbose)
                success.append(c)
            except KeyboardInterrupt:
                break
            except Exception as e:
                # If no data error, add the coin ignore list
                if re.match(CRYPTOCOMPARE_NO_DATA_ERROR, str(e)):
                    with open(self.path_coin_ignore, "a") as f:
                        f.write(c + "\n")
                    self.log.warning("No data for the symbol %s, "
                                     "coin added to ignore list." % c)
                else:
                    self.log.error("Failed to scrap coin %s: %s" % (c, str(e)))
        self.log.info("Successfully scraped %s %s data for %d coins"
                      % (to_curr, rate, len(success)))

    # Individual coin scraping methods
    def scrap_coin_day(self, from_curr, to_curr="BTC", update=True, verbose=1):
        """
        Dump CryptoCompare HistoDay data into a csv.
        """
        if verbose:
            self.log.info("Scraping daily data of market %s-%s..."
                          % (from_curr, to_curr))
        self.wait_for_internet_connection(INTERNET_CHECK_RATE)
        filename = from_curr + "-" + to_curr + ".csv"
        csv_path = os.path.join(self.path_day, filename)

        data = histo_day(from_curr, to_curr, all_data=True)
        df = pd.DataFrame(data, columns=CSV_HEADER)
        df["time"] = pd.to_datetime(df["time"], unit='s')
        # TOFIX: case when volumeto is 0 (last row for instance)

        df.to_csv(csv_path, index=False)

    def scrap_coin_hour(self, from_curr, to_curr="BTC",
                        update=True, verbose=1):
        """
        Dump CryptoCompare HistoHour data into a csv.

        If update = True, the scraper will first try to load
        existing data in the root path, and retrieve only the missing
        data from CryptoCompare.
        """
        if verbose:
            self.log.info("Scraping hourly data of market %s-%s..."
                          % (from_curr, to_curr))
        self.wait_for_internet_connection(INTERNET_CHECK_RATE)
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
                          update=True, verbose=1):
        """
        Dump CryptoCompare HistoMinute data into a csv.

        If update = True, the scraper will first try to load
        existing data in the root path, and retrieve only the missing
        data from CryptoCompare.
        """
        if verbose:
            self.log.info("Scraping minute data of market %s-%s..."
                          % (from_curr, to_curr))
        self.wait_for_internet_connection(INTERNET_CHECK_RATE)
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

    def get_active_coin_list(self, verbose=1):
        """
        Return a list of active coins, sorted by market cap.
        To be eligible, the coin need to be referenced on both
        Cryptocompare and Coinmarketcap, and not be referenced
        in coin_ignore_list.csv.

        :return: list
        """
        self.wait_for_internet_connection(INTERNET_CHECK_RATE)

        # Retrieve coins to ignore
        ignore_list = pd.read_csv(self.path_coin_ignore, header=None)

        # Retrieve active coin list from CoinMarketCap,
        # sorted by marketcap, and remove ignore list
        df = pd.read_json(COINMARKETCAP_TICKER_URL)
        len_CMC = len(df.symbol)
        df = df[~df.symbol.isin(ignore_list[0].values)]
        active_list = df.symbol.tolist()

        # Retrieve coin list from CryptoCompare
        cc_list = list(coin_list()["Data"].keys())

        # Solve different naming issues
        # (for instance: CryptoCompare IOT = CoinMarketCap MIOTA)
        for idx, crypto in enumerate(active_list):
            if crypto in list(COINMARKETCAP_TO_CRYPTOCOMPARE.keys()):
                active_list[idx] = COINMARKETCAP_TO_CRYPTOCOMPARE[crypto]

        # Intersection of CoinMarketCap and CryptoCompare list
        inter_list = [k for k in active_list if k in cc_list]

        if verbose:
            self.log.info("%d coins available on CoinMarketCap" % (len_CMC))
            self.log.info("%d coins available on CryptoCompare"
                          % (len(cc_list)))
            self.log.info("%d coins on ignore list" % (len(ignore_list)))
            self.log.info("%d active coins available for scraping"
                          % len(inter_list))

        return inter_list

    def check_for_updates(self, rate, to_curr, max_timedelta):
        return True

    def wait_for_internet_connection(self, check_rate):
        while not is_connected(REMOTE_SERVER):
            self.log.info("No internet connection. Trying again in %d min..."
                          % int(check_rate / 60))
            time.sleep(check_rate)


# Utils
def ts_to_str(ts):
    """Convert timestamp to datetime string"""
    return datetime.fromtimestamp(int(ts)).strftime(DATE_FORMAT)


def str_to_ts(date_str, str_format=DATE_FORMAT):
    """Convert datetime string to timestamp"""
    dt = datetime.strptime(date_str, str_format)
    return time.mktime(dt.timetuple())


def is_connected(hostname):
    """Check if internet connection is available"""
    try:
        # see if we can resolve the host name -- tells us if there is
        # a DNS listening
        host = socket.gethostbyname(hostname)
        # connect to the host -- tells us if the host is actually
        # reachable
        socket.create_connection((host, 80), 2)
        return True
    except:
        return False
