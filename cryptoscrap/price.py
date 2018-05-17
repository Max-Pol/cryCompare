import requests

__cl_url = 'https://www.cryptocompare.com/api/data/coinlist/'
__p_url = 'https://min-api.cryptocompare.com/data/price?'
__pm_url = 'https://min-api.cryptocompare.com/data/pricemulti?'
__pmf_url = 'https://min-api.cryptocompare.com/data/pricemultifull?'
__avg_url = 'https://min-api.cryptocompare.com/data/generateAvg?'
__davg_url = 'https://min-api.cryptocompare.com/data/dayAvg?'
__h_url = 'https://min-api.cryptocompare.com/data/pricehistorical?'
__cs_url = 'https://www.cryptocompare.com/api/data/coinsnapshot/?'
__csf_url = 'https://www.cryptocompare.com/api/data/coinsnapshotfullbyid/?'
__tp_url = 'https://min-api.cryptocompare.com/data/top/pairs?'


def coin_list():
    """
    Get general info for all the coins available on the website.

    :return: dict
    """
    return __get_url(__cl_url)


def price(from_curr, to_curr, e=None, extra_params=None,
          sign=False, try_conversion=True):
    """
    Get the latest price for a list of one or more currencies.
    Really fast, 20-60 ms. Cached each 10 seconds

    :param from_curr: From symbol
    :param to_curr: To symbol
    :param e: Name of exchanges, include multiple
    :param extra_params: Name of your application
    :param sign: If set to true, the server will sign the requests.
    :param try_conversion

    :return: dict
    """
    return __get_price(__p_url, from_curr, to_curr, e, extra_params,
                       sign, try_conversion)


def price_multi(from_curr, to_curr, e=None, extra_params=None,
                sign=False, try_conversion=True):
    """
    Get a matrix of currency prices.

    :param from_curr: From symbol
    :param to_curr: To symbol
    :param e: Name of exchanges, include multiple
    :param extra_params: Name of your application
    :param sign: If set to true, the server will sign the requests.
    :param try_conversion: (default True )If set to false, it will try to get
    values without using any conversion at all

    :return: dict
    """
    return __get_price(__pm_url, from_curr, to_curr, e, extra_params,
                       sign, try_conversion)


def price_multi_full(from_curr, to_curr, e=None, extra_params=None,
                     sign=False, try_conversion=True):
    """
    Get all the current trading info (price, vol, open, high, low etc)
    of any list of cryptocurrencies in any other currency that you need.
    If the crypto does not trade directly into the toSymbol requested,
    BTC will be used for conversion.

    :param from_curr: From symbol
    :param to_curr: To symbol
    :param e: Name of exchanges, include multiple
    :param extra_params: Name of your application
    :param sign: If set to true, the server will sign the requests.
    :param try_conversion: (default True )If set to false, it will try to get
    values without using any conversion at all

    :return: dict
    """
    return __get_price(__pmf_url, from_curr, to_curr, e, extra_params,
                       sign, try_conversion)


def price_historical(from_curr, to_curr, markets, ts=None, e=None,
                     extra_params=None, sign=False, try_conversion=True):
    """
    Get the price of any cryptocurrency in any other currency that you need
    at a given timestamp. The price comes from the daily info - so it would be
    the price at the end of the day GMT based on the requested TS.

    :param from_curr: From symbol
    :param to_curr: To symbol
    :param markets: Name of exchanges. Include multiple
    :param ts: Time stamp
    :param e: Name of exchanges, include multiple
    :param extra_params: Name of your application
    :param sign: If set to true, the server will sign the requests.
    :param try_conversion: (default True )If set to false, it will try to get
    values without using any conversion at all

    :return: dict
    """
    return __get_price(__h_url, from_curr, to_curr, markets, ts, e,
                       extra_params, sign, try_conversion)


def generate_avg(from_curr, to_curr, e, extra_params=None,
                 sign=False, try_conversion=True):
    """
    Get the price of any cryptocurrency in any other currency that you need
    at a given timestamp. The price comes from the daily info - so it would be
    the price at the end of the day GMT based on the requested TS.

    :param from_curr: From symbol
    :param to_curr: To symbol
    :param e: Name of exchanges, include multiple
    :param extra_params: Name of your application
    :param sign: If set to true, the server will sign the requests.
    :param try_conversion: (default True )If set to false, it will try to get
    values without using any conversion at all

    :return: dict
    """
    return __get_avg(__avg_url, from_curr, to_curr, e, extra_params,
                     sign, try_conversion)


def day_avg(from_curr, to_curr, e=None, extra_params=None, sign=False,
            try_conversion=True, avg_type=None, utc_diff=0, to_ts=None):
    """
    Get day average price. The values are based on hourly vwap data and the
    average can be calculated in different waysIt uses BTC conversion if data
    is not available because the coin is not trading in the specified currency.
    If tryConversion is set to false it will give you the direct data.
    If no toTS is given it will automatically do the current day. Also for
    different timezones use the UTCHourDiff paramThe calculation types are:
    HourVWAP - a VWAP of the hourly close price,MidHighLow - the average
    between the 24 H high and low. VolFVolT - the total volume from / the total
    volume to (only avilable with tryConversion set to false so only for direct
    trades but the value should be the most accurate price.

    :param from_curr: From symbol
    :param to_curr: To symbol
    :param e: Name of exchanges, include multiple
    :param extra_params: Name of your application
    :param sign: If set to true, the server will sign the requests.
    :param try_conversion: (default True )If set to false, it will try to get
    values without using any conversion at all
    :param avg_type: [ HourVWAP, MidHighLow, VolFVolT ]
    :param utc_diff: By deafult it does UTC, if you want a different time zone
    just pass the hour difference. For PST you would pass -8 for example.
    :param to_ts: Hour unit

    :return: dict
    """
    return __get_avg(__davg_url, from_curr, to_curr, e, extra_params, sign,
                     try_conversion, avg_type, utc_diff, to_ts)


def coin_snapshot(from_curr, to_curr):
    """
    Get data for a currency pair. It returns general block explorer information,
     aggregated data and individual data for each exchange available.

    :param from_curr: From symbol
    :param to_curr: To symbol

    :return: dict
    """
    return __get_url(__cs_url + 'fsym=' + from_curr.upper() +
                     '&tsym=' + to_curr.upper())


def coin_snapshot_id(coin_id):
    """
    CoinSnapshotFullById

    Get the general, subs (used to connect to the streamer and to figure out
    what exchanges we have data for and what are the exact coin pairs
    of the coin) and the aggregated prices for all pairs available.

    :param coin_id

    :return: dict
    """
    return __get_url(__csf_url + 'id=' + str(coin_id))


def top_pairs(from_curr, to_curr=None, limit=None, sign=None):
    """
    TopPairs

    Get top pairs by volume for a currency (always uses our aggregated data).
    The number of pairs you get is the minimum of the limit you set (default 5)
    and the total number of pairs available.

    :param from_curr: From symbol
    :param to_curr: To symbol
    :param limit: Number of pairs returned. Max 2000.
    :param sign: If set to true, the server will sign the requests.

    :return: dict
    """
    return __get_top_pairs(__tp_url, from_curr, to_curr, limit, sign)


def __get_price(base_url, from_curr, to_curr, e=None, extra_params=None,
                sign=False, try_conversion=True, markets=None, ts=None):

    args = list()
    if isinstance(from_curr, str):
        args.append('fsym=' + from_curr.upper())
    elif isinstance(from_curr, list):
        args.append('fsyms=' + ','.join(from_curr).upper())
    if isinstance(to_curr, list):
        args.append('tsyms=' + ','.join(to_curr).upper())
    elif isinstance(to_curr, str):
        args.append('tsyms=' + to_curr.upper())
    if isinstance(markets, str):
        args.append('markets=' + markets)
    elif isinstance(markets, list):
        args.append('markets=' + ','.join(markets))
    if e:
        args.append('e=' + e)
    if extra_params:
        args.append('extraParams=' + extraParams)
    if sign:
        args.append('sign=true')
    if ts:
        args.append('ts=' + str(ts))
    if not try_conversion:
        args.append('tryConversion=false')

    if len(args) >= 2:
        return __get_url(base_url + '&'.join(args))
    else:
        raise ValueError('Must have both fsym and tsym arguments.')


def __get_avg(base_url, from_curr, to_curr, markets=None, e=None,
              extra_params=None, sign=False, try_conversion=True, avg_type=None,
              utc_diff=0, to_ts=None):

    args = list()
    if isinstance(from_curr, str):
        args.append('fsym=' + from_curr.upper())
    if isinstance(to_curr, str):
        args.append('tsym=' + to_curr.upper())
    if isinstance(markets, str):
        args.append('markets=' + markets)
    elif isinstance(markets, list):
        args.append('markets=' + ','.join(markets))
    if e:
        args.append('e=' + e)
    if extra_params:
        args.append('extraParams=' + extraParams)
    if sign:
        args.append('sign=true')
    if avg_type:
        args.append('avgType=' + avgType)
    if utc_diff:
        args.append('UTCHourDiff=' + str(UTCHourDiff))
    if to_ts:
        args.append('toTs=' + toTs)
    if not try_conversion:
        args.append('tryConversion=false')

    if len(args) >= 2:
        return __get_url(base_url + '&'.join(args))
    else:
        raise ValueError('Must have both fsym and tsym arguments.')


def __get_top_pairs(base_url, from_curr, to_curr, limit, sign):

    args = list()
    if isinstance(from_curr, str):
        args.append('fsym=' + from_curr.upper())
    if isinstance(to_curr, str):
        args.append('fsym=' + from_curr.upper())
    if limit:
        args.append('limit=' + str(limit))
    if sign:
        args.append('sign=true')

    if len(args) >= 1:
        return __get_url(base_url + '&'.join(args))
    else:
        raise ValueError('Must set fsym argument.')


def __get_url(url):
    raw_data = requests.get(url)
    raw_data.encoding = 'utf-8'
    if raw_data.status_code != 200:
        raw_data.raise_for_status()
        return False
    try:
        if raw_data.json()['Response'] != "Success":
            raise ValueError('Cryptocompare API Error: %s' % raw_data.json()['Message'])
        return raw_data.json()
    except NameError:
        raise ValueError('Cannot parse to json.')
