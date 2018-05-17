import requests

__histominuteurl = 'https://min-api.cryptocompare.com/data/histominute?'
__histohoururl = 'https://min-api.cryptocompare.com/data/histohour?'
__histodayurl = 'https://min-api.cryptocompare.com/data/histoday?'


def histo_minute(from_curr, to_curr, e=None, extra_params=None, sign=False,
                 try_conversion=True, aggregate=None, limit=None, to_ts=None):
    """
    Get open, high, low, close, volumefrom and volumeto from the each minute
    historical data. This data is only stored for 7 days, if you need more,
    use the hourly or daily path.

    :param from_curr From symbol (fsym)
    :param to_curr To symbol (tsym)
    :param e: Exchange (str)
    :param extra_params: ****check this*******
    :param sign
    :param try_conversion
    :param aggregate
    :param limit
    :param to_ts
    """
    return __get_price(__histominuteurl, from_curr, to_curr, e, extra_params,
                       sign, try_conversion, aggregate, limit, to_ts)


def histo_hour(from_curr, to_curr, e=None, extra_params=None, sign=False,
               try_conversion=True, aggregate=None, limit=None, to_ts=None):
    """
    Get open, high, low, close, volumefrom and volumeto from the each hour
    historical data. It uses BTC conversion if data is not available
    because the coin is not trading in the specified currency.

    :param from_curr From symbol (fsym)
    :param to_curr To symbol (tsym)
    :param e: Exchange (str)
    :param extra_params: ****check this*******
    :param sign
    :param try_conversion
    :param aggregate
    :param limit
    :param to_ts
    """
    return __get_price(__histohoururl, from_curr, to_curr, e, extra_params,
                       sign, try_conversion, aggregate, limit, to_ts)


def histo_day(from_curr, to_curr, e=None, extra_params=None, sign=False,
              try_conversion=True, aggregate=None, limit=None, to_ts=None,
              all_data=False):
    """
    Get open, high, low, close, volumefrom and volumeto daily historical data.
    The values are based on 00:00 GMT time.

    :param from_curr From symbol (fsym)
    :param to_curr To symbol (tsym)
    :param e: Exchange (str)
    :param extra_params: ****check this*******
    :param sign
    :param try_conversion
    :param aggregate
    :param limit
    :param to_ts
    :param all_data
    """
    return __get_price(__histodayurl, from_curr, to_curr, e, extra_params, sign,
                       try_conversion, aggregate, limit, to_ts, all_data)


def __get_price(base_url, from_curr, to_curr, e=None, extra_params=None,
                sign=False,
                try_conversion=True, aggregate=None, limit=None, to_ts=None,
                all_data=False):
    args = list()
    if isinstance(from_curr, str):
        args.append('fsym=' + from_curr.upper())
    if isinstance(to_curr, str):
        args.append('tsym=' + to_curr.upper())
    if e:
        args.append('e=' + e)
    if extra_params:
        args.append('extraParams=' + extra_params)
    if sign:
        args.append('sign=true')
    if aggregate:
        args.append('aggregate=' + str(aggregate))
    if limit:
        args.append('limit=' + str(limit))
    if to_ts:
        args.append('toTs=' + str(to_ts))
    if all_data:
        args.append('allData=true')
    if not try_conversion:
        args.append('tryConversion=false')
    if len(args) >= 2:
        return __get_url(base_url + '&'.join(args))
    else:
        raise ValueError('Must have both fsym and tsym arguments.')


def __get_url(url):
    raw_data = requests.get(url)
    raw_data.encoding = 'utf-8'
    if raw_data.status_code != 200:
        raw_data.raise_for_status()
        return False
    try:
        if raw_data.json()['Response'] != "Success":
            raise ValueError('Cryptocompare API Error: %s' % raw_data.json()['Message'])
        return raw_data.json()['Data']
    except NameError:
        raise ValueError('Cannot parse to json.')
