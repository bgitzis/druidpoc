import json
from datetime import datetime

import requests
from pydruid.client import PyDruid
from pydruid.utils.filters import Dimension

__author__ = 'Barak Gitsis'

TRANQUILITY_URL = 'http://192.168.160.128:8200/v1/post/pageviews1'
DRUID_URL = 'http://192.168.160.128:8082'


def query_druid():
    client = PyDruid(DRUID_URL, 'druid/v2')
    query = client.select(
        datasource='pageviews1',
        granularity='all',
        dimensions=["url", "user"],
        filter=Dimension('user') == 'ethan',
        paging_spec={"pagingIdentifiers": {}, "threshold": 5},
        intervals=["2016-07-08/2017-09-13"]
    )
    # print json.dumps(query.result, indent=2)
    return query.result


def load_record():
    # {"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "bob" , "latencyMs": 45}
    # {"time": "2016-07-09T17:03:33Z", "url": "/foo/bar", "user": "phil", "latencyMs": 32}
    record = {"time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
              "url": "/foo/bar",
              "user": "ethan",
              "latencyMs": 32}
    payload = json.dumps(record)
    print payload
    load_response = requests.post(url=TRANQUILITY_URL, headers={'Content-Type': 'application/json'}, data=payload)
    print load_response.json()
    print "[%d] %s\n" % (load_response.status_code, load_response.text)


def main():
    print 'Start!'
    load_record()
    events = []
    for segment_result in query_druid():
        events.extend(segment_result['result']['events'])
    if len(events) is 0:
        print "No events found"
    else:
        print len(events)


if __name__ == '__main__':
    # print two_minutes_ago().strftime("%Y-%m-%dT%H:%M:%SZ")
    main()
