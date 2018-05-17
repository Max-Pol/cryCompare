import requests

__socialurl = 'https://www.cryptocompare.com/api/data/socialstats/?'
__miningurl = 'https://www.cryptocompare.com/api/data/miningequipment/'


def social_stats(coin_id):
    """
    Get CryptoCompare website, Facebook, code repository, Twitter and Reddit
    data for coins. If called with the id of a cryptopian you just get data
    from CryptoCompare website that is available to the public.

    :param coin_id: Id of a coin
    :return: dict
    """
    return __get_data(__socialurl, coin_id)


def mining_equipment():
    """
    Get all the mining equipment available on the website.

    :return: dict
    """
    return __get_url(__miningurl)


def __get_data(urlbase, id):
    url = urlbase + 'id=' + str(id)
    raw_data = requests.get(url)
    raw_data.encoding = 'utf-8'
    if raw_data.status_code != 200:
        raw_data.raise_for_status()
        return False
    try:
        return raw_data.json()['Data']
    except NameError:
        raise ValueError('Cannot parse to json.')


def __get_url(url):
    raw_data = requests.get(url)
    raw_data.encoding = 'utf-8'
    if raw_data.status_code != 200:
        raw_data.raise_for_status()
        return False
    try:
        return raw_data.json()
    except NameError:
        raise ValueError('Cannot parse to json.')
