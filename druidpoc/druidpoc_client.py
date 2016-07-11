import json

import requests
from pydruid.client import PyDruid

from druidpoc.batch_load_druid import base_path, OVERLORD_URL


class DruidPocClient(PyDruid):
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
        indexing_task_spec['spec']['ioConfig']['inputSpec']['paths'] = DruidPocClient.in_vm_dir + '/' + file_name

        with open(DruidPocClient.events_dir + '/' + file_name, 'w') as events_fh:
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


class DruidPocException(Exception):
    pass
