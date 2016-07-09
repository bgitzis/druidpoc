import json
from pprint import pprint

import requests
from pydruid.client import PyDruid
from pydruid.utils.filters import Dimension

from druidpoc.model import MarketEvent

__author__ = 'Barak Gitsis'

TABLE_NAME = 'marketevents'
TRANQUILITY_URL = 'http://192.168.160.128:8200/v1/post'
DRUID_URL = 'http://192.168.160.128:8082'


def check_immediate_availability():
    # start indexing task - should take a few seconds
    # post_to_tranquility(MarketEvent('trade', 'gold', 'c1', 500, 'Russia', -10, 16.7))
    product_name = 'bronze'
    post_to_tranquility(MarketEvent('trade', product_name, 'c1', 500, 'Russia', -10, 16.7))
    events = select_one_market_event(product_name)
    return len(events) > 0


def select_one_market_event(product_name):
    client = PyDruid(DRUID_URL, 'druid/v2')
    query = client.select(
        datasource=TABLE_NAME,
        granularity='all',
        dimensions=['product_name'],
        filter=Dimension('product_name') == product_name,
        paging_spec={"pagingIdentifiers": {}, "threshold": 1},
        intervals=["2016-07-08/2017-09-13"]
    )
    return [segment_result['result']['events'] for segment_result in query.result]


def post_to_tranquility(me):
    payload = json.dumps(me.__dict__)
    print payload
    load_response = requests.post(url=TRANQUILITY_URL + '/' + TABLE_NAME,
                                  headers={'Content-Type': 'application/json'},
                                  data=payload)
    print "[%d] %s\n" % (load_response.status_code, load_response.text)


def main():
    results = {'immediate_availability': check_immediate_availability()}
    pprint(results)


if __name__ == '__main__':
    main()
