from druidpoc.me_druid_client import MeDruidHelper


def track_indexing_task():
    """
    example of tracking indexing task (either batch or streaming, although streaming will run until period closes)
    :return:
    """
    MeDruidHelper.track_indexing_task(
        'http://192.168.160.128:8090/druid/indexer/v1/task/index_hadoop_marketevents_2016-07-11T09:38:07.348Z/status')
