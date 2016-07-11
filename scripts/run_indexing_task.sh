#!/usr/bin/env bash
if [ -z ${druid_ip+x} ] ; then druid_ip=192.168.160.128 ; fi
curl -s XPOST -H'Content-Type: application/json' --data-binary @resources/index_tasks/market_event_index.json http://${druid_ip}:8090/druid/indexer/v1/task
