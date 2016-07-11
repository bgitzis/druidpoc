from pprint import pprint

from druidpoc.me_druid_client import MeDruidClient
from druidpoc.model import MarketEvent

__author__ = 'Barak Gitsis'

TABLE_NAME = 'marketevents'


def check_immediate_availability():
    # start indexing task - should take a few seconds
    # post_to_tranquility(MarketEvent('trade', 'gold', 'c1', 500, 'Russia', -10, 16.7))
    product_name = 'bronze'
    MeDruidClient.post_to_tranquility(MarketEvent('trade', product_name, 'c1', 500, 'Russia', -10, 16.7), TABLE_NAME)
    events = MeDruidClient.select_one_market_event(product_name)
    return len(events) > 0


def main():
    results = {'immediate_availability': check_immediate_availability()}
    pprint(results)


if __name__ == '__main__':
    main()
