CREATE DATABASE IF NOT EXISTS openrec;

CREATE EXTERNAL TABLE IF NOT EXISTS openrec.user_entity (json STRING)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/openrec/hive/user';

CREATE EXTERNAL TABLE IF NOT EXISTS openrec.item_entity (json STRING)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/openrec/hive/item';

CREATE EXTERNAL TABLE IF NOT EXISTS openrec.event_entity (json STRING)
PARTITIONED BY (dt STRING)
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/openrec/hive/event';

-- Existing partitions can be registered once with MSCK REPAIR TABLE. Daily jobs add only the
-- requested partition with ALTER TABLE ADD IF NOT EXISTS, avoiding a full filesystem scan.

-- Example typed projection for algorithm jobs:
-- SELECT get_json_object(json, '$.userId') AS user_id,
--        get_json_object(json, '$.itemId') AS item_id,
--        get_json_object(json, '$.type') AS event_type,
--        CAST(get_json_object(json, '$.time') AS BIGINT) AS event_time
-- FROM openrec.event_entity;
