import json
import os
import time

import requests
from pydruid.client import PyDruid
from pydruid.utils.filters import Dimension

TRANQUILITY_URL = 'http://192.168.160.128:8200/v1/post'
DRUID_BROKER_URL = 'http://192.168.160.128:8082'
OVERLORD_URL = 'http://192.168.160.128:8090/druid/indexer/v1/task'

TABLE_NAME = 'marketevents'
base_path = '/'.join([os.path.split(__file__)[0], '..', 'resources', 'index_tasks'])


# TODO load urls from config and convert static methods to instance methods
class MeDruidHelper(PyDruid):
    """
    Market Events on Druid Helper
    Auxilary class for working with Market Events in Druid
    """
    events_dir = 'G:/work'
    in_vm_dir = '/mnt/hgfs/G/work'

    @staticmethod
    def index_market_events(file_name, market_events):
        """
        Creates data file from list of market_events at location accessible to Druid and submits indexing task

        :type file_name: Union[str,unicode]
        :type market_events: list

        :param file_name: name of the data file
        :param market_events: list of events
        :return:
        """

        task_proto_path = base_path + '/market_event_indexing_task_proto.json'
        with open(task_proto_path) as fh:
            indexing_task_spec = json.loads(fh.read())
        if indexing_task_spec is None:
            raise DruidPocException('unable to load indexing task proto from ' + task_proto_path)

        # model for indexing task is needed for production use
        indexing_task_spec['spec']['ioConfig']['inputSpec']['paths'] = MeDruidHelper.in_vm_dir + '/' + file_name

        with open(MeDruidHelper.events_dir + '/' + file_name, 'w') as events_fh:
            for event in market_events:
                events_fh.write(json.dumps(vars(event), sort_keys=True) + '\n')

        MeDruidHelper.synchronous_indexing_task(indexing_task_spec)

    @staticmethod
    def synchronous_indexing_task(indexing_task_spec):
        submit_response = requests.post(OVERLORD_URL, headers={'Content-Type': 'application/json'},
                                        data=json.dumps(indexing_task_spec))
        if submit_response.status_code == 200 and submit_response.reason == 'OK':
            task_id = json.loads(submit_response.text)['task']
            tracking_url = '%s/%s/status' % (OVERLORD_URL, task_id)
            print 'Indexing should begin shortly. Tracking URL: %s' % tracking_url
            MeDruidHelper.track_indexing_task(task_id)
        else:
            print 'Failed submitting task, reason:' + submit_response.reason

    @staticmethod
    def track_indexing_task(task_id):
        tracking_url = '%s/%s/status' % (OVERLORD_URL, task_id)
        status_response = requests.get(tracking_url)
        print status_response.json()
        task_status = status_response.json()['status']['status']
        while status_response.status_code == 200 and task_status not in ['SUCCESS', 'FAILED']:
            time.sleep(10)
            status_response = requests.get(tracking_url)
            task_status = status_response.json()['status']['status']
            print '[%d] %s - %s' % (status_response.status_code, task_status, status_response.json())

    @staticmethod
    def post_to_tranquility(record, table_name=TABLE_NAME):
        """
        used for streaming into Druid through tranquility
        :param record:
        :param table_name:
        :return:
        """
        payload = json.dumps(record.__dict__)
        print payload
        load_response = requests.post(url=TRANQUILITY_URL + '/' + table_name,
                                      headers={'Content-Type': 'application/json'},
                                      data=payload)
        print "[%d] %s\n" % (load_response.status_code, load_response.text)

    @staticmethod
    def shutdown_streaming_task(task_id):
        task_shutdown_url = '%s/%s/shutdown' % (OVERLORD_URL, task_id)
        response = requests.post(task_shutdown_url)
        print '[%d] %s' % (response.status_code, response.json())

    @staticmethod
    def select_one_market_event(product_name):
        client = PyDruid(DRUID_BROKER_URL, 'druid/v2')
        query = client.select(
            datasource=TABLE_NAME,
            granularity='all',
            dimensions=['product_name'],
            filter=Dimension('product_name') == product_name,
            paging_spec={"pagingIdentifiers": {}, "threshold": 1},
            intervals=["2016-07-08/2017-09-13"]
        )

        events = [segment_result['result']['events'] for segment_result in query.result]
        if len(events) >= 1:
            return events[0]
        return []


class DruidPocException(Exception):
    pass
