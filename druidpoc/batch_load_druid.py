import random
from copy import copy
from datetime import timedelta, datetime

from druidpoc import model
from druidpoc.me_druid_client import MeDruidHelper
from druidpoc.model import MarketEvent


def main():
    proto_event = MarketEvent('trade', 'copper', 'Sumitomo', 50000, 'Japan', qty=-10000, price=200)
    events = []
    for _ in range(1):
        me = copy(proto_event)
        random_int = random.randint(1, 100)
        me.time = (datetime.utcnow() - timedelta(days=random_int)).strftime(model.DT_FORMAT)
        me.qty = proto_event.qty + 1000 * random_int * (1 if random_int % 2 == 0 else -1)
        events.append(me)
    MeDruidHelper.index_market_events('marketevents2.json', events)


if __name__ == '__main__':
    main()
