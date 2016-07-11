#!/usr/bin/env bash
#for vm set IP, for local it is localhost
export druid_ip=192.168.160.128

#load from pageviews.json file


curl -XPOST -H'Content-Type: application/json' --data-binary @pageviews.json http://${druid_ip}:8200/v1/post/pageviews ; echo -e "\n$?"
load from stdin
echo '{"url": "/foo/bar", "latencyMs": 32, "user": "chris", "time": "2016-07-09T17:28:44Z"}' |
   curl -s -XPOST -H'Content-Type: application/json' --data-binary @- http://${druid_ip}:8200/v1/post/pageviews ; echo -e "\n$?"
#query
curl -XPOST -H'Content-Type: application/json' --data-binary @pageviews-select-all.json http://${druid_ip}:8082/druid/v2/?pretty ; echo -e "\n$?"
