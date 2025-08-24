curl -X POST "10.18.1.173:9200/_refresh"
curl -X GET "10.18.1.173:9200/_cat/indices?v"


curl -X DELETE "10.18.1.173:9200/test_security_events_aggs"
curl -X DELETE "10.18.1.173:9200/test_sevs_logs"
curl -X DELETE "10.18.1.173:9200/test_event_logs"
curl -X DELETE "10.18.1.173:9200/test_event_logs_aggs"
curl -X DELETE "10.18.1.173:9200/test_security_events"
curl -X DELETE "10.18.1.173:9200/test_event_logs_late"

curl -X DELETE "10.18.1.173:9200/logging_sevs"
curl -X DELETE "10.18.1.173:9200/logging_event_logs"
curl -X DELETE "10.18.1.173:9200/logging_event_logs_aggs"
curl -X DELETE "10.18.1.173:9200/logging_sevs_aggs"
curl -X DELETE "10.18.1.173:9200/can_ml_predictions"




for index in $(curl -s -X GET "10.18.1.173:9200/_cat/indices?h=index"); do
  if [[ $index == test_* ]]; then
    curl -X DELETE "10.18.1.173:9200/$index"
    echo "Deleted index: $index"
  fi
done

curl -X POST "10.18.1.173:9200/security_events/_refresh"

curl -X GET "10.18.1.173:9200/test_event_logs_aggs/_count?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match_all": {}
  }
}
'


curl -X GET "10.18.1.155:9200/ui_event_logs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match_all": {}
  }
}
'



curl -X GET "10.18.1.173:9200/ui_security_events_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 0,
  "aggs": {
    "total_count": {
      "sum": {
        "field": "count"
      }
    }
  }
}
'

curl -X GET "10.18.1.173:9200/ui_event_logs/_count?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "range": {
      "Timestamp": {
        "gte": "now-5m",
        "lt": "now"
      }
    }
  }
}
'

curl -X GET "10.18.1.173:9200/ui_security_events_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "range": {
      "Timestamp": {
        "gte": "now-500m",
        "lt": "now"
      }
    }
  }
}
'
curl -X GET "10.18.1.173:9200/ui_security_events_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 1,
  "sort": [
    { "window_start": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}
'

curl -X GET "10.18.1.173:9200/logging_event_logs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 1,
  "sort": [
    { "time_received_from_kafka": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}
'

curl -X GET "10.18.1.173:9200/logging_sevs_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 1,
  "sort": [
    { "time_received_from_kafka": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}
'

curl -X GET "http://10.18.1.173:9200/logging_sevs_aggs/_search?pretty" -H 'Content-Type: application/json' -d'
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


curl -X POST "10.18.1.173:9200/_reindex?pretty" -H 'Content-Type: application/json' -d'
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
}
'

curl -X GET "10.18.1.173:9200/test_event_logs/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "size": 5,
  "sort": [
    { "Timestamp": { "order": "desc" } }
  ],
  "query": {
    "match_all": {}
  }
}
'


curl -X GET "http://10.18.1.173:9200/_mapping?pretty" -H 'Content-Type: application/json'

curl -X GET "10.18.1.173:9200/api_logs/_mapping?pretty"