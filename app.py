import os
import time
import random
import datetime as dt
import logging
import sys
from cryptoscrap.scraper import Scraper


# Load config from environment
path_data = os.getenv('PATH_DATA', "/data")
delay_start = int(os.getenv('DELAY_START', 5 * 60))
delay_between_scrap = int(os.getenv('DELAY_BETWEEN_SCRAP', 3600))
refresh_rate = int(os.getenv('REFRESH_RATE', 3600 * 24))

# Set logger
log = logging.getLogger()
log.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


# Starting the app
log.info("Starting Cryptoscrap, let's scrap the hell out of it!")
time.sleep(delay_start)

# Initialize Scraper & shuffle currency order
s = Scraper(path_data, logger=log)
to_curr = ["USD", "BTC"]
random.shuffle(to_curr)

# Check and Scrap
if s.check_for_updates("minute", to_curr[0], dt.timedelta(days=1)):
        log.info("New data available for %s market" % to_curr[0])
        s.scrap('minute', to_curr[0], verbose=0)
        time.sleep(delay_between_scrap)

if s.check_for_updates("minute", to_curr[1], dt.timedelta(days=1)):
        log.info("New data available for %s market" % to_curr[1])
        s.scrap('minute', to_curr[1], verbose=0)


# Perform updates at refresh_rate
while True:
    log.info("Waiting for next scraping session (every %d hour)..."
             % int(refresh_rate / 3600))
    time.sleep(refresh_rate)

    # Scrap
    random.shuffle(to_curr)
    s.scrap('minute', to_curr[0], verbose=0)
    time.sleep(delay_between_scrap)
    s.scrap('minute', to_curr[1], verbose=0)
