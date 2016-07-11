from pprint import pprint

from druidpoc.me_druid_client import MeDruidHelper
from druidpoc.model import MarketEvent

__author__ = 'Barak Gitsis'


def check_immediate_availability():
    # start indexing task - should take a few seconds
    # post_to_tranquility(MarketEvent('trade', 'gold', 'c1', 500, 'Russia', -10, 16.7))
    product_name = 'bronze'
    MeDruidHelper.post_to_tranquility(MarketEvent('trade', product_name, 'c1', 500, 'Russia', -10, 16.7))
    events = MeDruidHelper().select_one_market_event(product_name)
    return len(events) > 0


def check_as_of_query():
    return False


def main():
    results = {'as_of_query': check_as_of_query()}
    pprint(results)


if __name__ == '__main__':
    main()
