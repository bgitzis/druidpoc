import random
import time
from copy import copy
from datetime import datetime, timedelta
from unittest import TestCase, skip

from dateutil.relativedelta import relativedelta

from druidpoc import model
from druidpoc.me_druid_client import MeDruidHelper
from druidpoc.model import MarketEvent


# noinspection PyMethodMayBeStatic
class ApiDemo(TestCase):
    def test_batch_load(self):
        proto_event = MarketEvent('trade', 'copper', 'Sumitomo', 50000, 'Japan', qty=-10000, price=200)
        # generate a few events
        events = []
        for _ in range(100):
            me = copy(proto_event)
            random_int = random.randint(1, 100)
            me.time = (datetime.utcnow() - timedelta(days=random_int)).strftime(model.DT_FORMAT)
            me.qty = proto_event.qty + 1000 * random_int * (1 if random_int % 2 == 0 else -1)
            events.append(me)

        MeDruidHelper.index_market_events('marketevents2.json', events)

    def test_track_indexing_task(self):
        """
        example of tracking indexing task (either batch or streaming, although streaming will run until period closes)
        :return:
        """
        MeDruidHelper.track_indexing_task('index_hadoop_marketevents_2016-07-11T09:38:07.348Z')

    def test_simple_select(self):
        product_name = 'copper'
        event_time = (datetime.utcnow() - relativedelta(month=1)).strftime(model.DT_FORMAT)
        event = MarketEvent('trade', product_name, 'Sumitomo', 50000, 'Japan', qty=-10000, price=200, time=event_time)
        MeDruidHelper.index_market_events('marketevents2.json', [event])
        print MeDruidHelper().select_one_market_event(product_name)

    def test_shutdown_streaming_task(self):
        MeDruidHelper.shutdown_streaming_task('index_realtime_marketevents_2016-07-11T10%3A00%3A00.000Z_0_0')

    @skip("test separately, after batch indexing is complete")
    def test_streaming(self):
        product_name = 'ore'
        MeDruidHelper.post_to_tranquility(MarketEvent('trade', product_name, 'South Land', 500, 'Russia', -10, 16.7))
        time.sleep(1)
        event = MeDruidHelper().select_one_market_event(product_name)
        print event

    def test_positions_query(self):
        delta = MeDruidHelper().positions_delta(product_name='copper', min_num_employees=1000,
                                                start_dt=datetime(year=2016, month=02, day=01),
                                                end_dt=datetime(year=2016, month=07, day=10))

        print delta
