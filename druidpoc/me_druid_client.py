import json

import requests
from pydruid.client import PyDruid
from pydruid.utils.filters import Dimension

from druidpoc.batch_load_druid import base_path
from druidpoc.functionality_checks import TABLE_NAME

TRANQUILITY_URL = 'http://192.168.160.128:8200/v1/post'
DRUID_BROKER_URL = 'http://192.168.160.128:8082'
OVERLORD_URL = 'http://192.168.160.128:8090/druid/indexer/v1/task'


class MeDruidHelper(PyDruid):
    """
    Auxilary class for working with Market Events in Druid
    """
    events_dir = 'G:/work'
    in_vm_dir = '/mnt/hgfs/G/work'

    @staticmethod
    def submit_indexing_task(file_name, market_events):
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

        response = requests.post(OVERLORD_URL, headers={'Content-Type': 'application/json'},
                                 data=json.dumps(indexing_task_spec))
        if response.status_code == 200 and response.reason == 'OK':
            task_id = json.loads(response.text)['task']
            print 'Indexing should begin shortly. Tracking URL: %(overlord_url)s/%(task_id)s/status' % {
                'overlord_url': OVERLORD_URL,
                'task_id': task_id
            }
        else:
            print 'Failed submitting task, reason:' + response.reason

    @staticmethod
    def post_to_tranquility(record, table_name):
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
        return [segment_result['result']['events'] for segment_result in query.result]


class DruidPocException(Exception):
    pass
