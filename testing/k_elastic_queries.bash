#get the password for the elasticsearch user
PASSWORD=$(kubectl get secret uraeus-elastic-search-es-elastic-user -n dev -o go-template='{{.data.elastic | base64decode}}')
echo $PASSWORD


# Check segment counts
GET _cat/segments/test_event_logs?v

# Check merge stats
GET _stats/merge?pretty

# Monitor node stats
GET _nodes/stats/indices/merge?pretty

curl -u "elastic:MS8oTy721L61EcZiiMg9M281"  "https://elastic.dev.uraeusdev.com:443/_refresh"
curl -u "elastic:0NfAKg6QaIpQ7430htm9805s" -X GET "https://elastic.dev.uraeusdev.com:443/_cat/nodes?v"

curl -u "elastic:MS8oTy721L61EcZiiMg9M281"  "http://10.18.1.155:30920/_refresh"
curl -u "elastic:Tc3o1a1yP6G9d0d4DOm815Im"  "http://localhost:9200/_cat/indices?v"


curl -u "elastic:0NfAKg6QaIpQ7430htm9805s"  -X DELETE "https://elastic.dev.uraeusdev.com:443/ui_event_logs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/test_event_logs_aggs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/test_security_events_aggs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/test_sevs_logs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/test_security_events"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/test_event_logs_late"

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/logging_sevs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/logging_event_logs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/logging_event_logs_aggs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/logging_sevs_aggs"
curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/can_ml_predictions"

for index in $(curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -s -X GET "https://elastic.dev.uraeusdev.com:443/_cat/indices?h=index"); do
  if [[ $index == test_* ]]; then
    curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X DELETE "https://elastic.dev.uraeusdev.com:443/$index"
    echo "Deleted index: $index"
  fi
done

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X POST "https://elastic.dev.uraeusdev.com:443/security_events/_refresh"

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/test_event_logs_aggs/_count?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match_all": {}
  }
}'


curl -u "elastic:MKgGfMrlt10X795O36oQ3D41"  -X GET "https://localhost:9200/ui_security_events/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match_all": {}
  }
}'

curl -u "elastic:0NfAKg6QaIpQ7430htm9805s"  -X GET "http://10.18.1.155:30920/ui_security_events_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggs": {
    "total_count": {
      "sum": {
        "field": "count"
      }
    }
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/ui_event_logs/_count?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "range": {
      "Timestamp": {
        "gte": "now-5m",
        "lt": "now"
      }
    }
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/ui_security_events_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "range": {
      "Timestamp": {
        "gte": "now-500m",
        "lt": "now"
      }
    }
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/ui_security_events_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 1,
  "sort": [
    { "window_start": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/logging_event_logs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 1,
  "sort": [
    { "time_received_from_kafka": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/logging_sevs_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 1,
  "sort": [
    { "time_received_from_kafka": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/logging_sevs_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 1,
  "sort": [
    { "time_received_from_kafka": { "order": "desc" } }
  ],
  "fields": [
    "job_type",
    "step_id",
    "batch_number",
    "processing_time_in_seconds",
    "total_latency_in_seconds",
    "average_processing_time_last_100_batches",
    "average_total_latency_last_100_batches",
    "total_messages_received_from_kafka_so_far"
  ],
  "_source": true
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X POST "https://elastic.dev.uraeusdev.com:443/_reindex?pretty" -H 'Content-Type: application/json' -d'
{
  "source": {
    "index": "security_events"
  },
  "dest": {
    "index": "ui_security_events"
  },
  "script": {
    "source": "ctx._id = ctx._source.Alert_ID"
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/test_event_logs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 5,
  "sort": [
    { "Timestamp": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/_mapping?pretty" -H 'Content-Type: application/json'

curl -u "elastic:MxF54jP664Iqx4i61t2J1JHn"  -X GET "https://elastic.dev.uraeusdev.com:443/ui_security_events/_mapping?pretty"




