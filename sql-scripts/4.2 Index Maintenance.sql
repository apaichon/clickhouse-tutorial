 /*
 System Tables for Monitoring
 Slide Page : 104
 */

 Show PROCESSLIST
 

 select 
    initial_query_id,type,
    event_date,event_time,query_kind, 
    query_duration_ms,result_rows, 
    result_bytes, memory_usage, query 
    from system.query_log
    -- select * from system.query_log


CREATE TABLE events (
    event_id UInt64,
    event_type String,
    event_time DateTime,
    user_id UInt32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, event_id);


INSERT INTO events (event_id, event_type, event_time, user_id)
SELECT
    number + 1 AS event_id,
    arrayElement(['purchase', 'view', 'click', 'search'],
        rand64() % 4) AS event_type,
    today() - rand() % 365 AS event_time,
    rand64() % 10000 AS user_id
FROM numbers(1000000);

Alter table events  add index idx_event_date event_time type minmax GRANULARITY 10000;


SELECT
    partition,
    name,
    rows,
    active
FROM system.parts
WHERE table = 'events'

OPTIMIZE TABLE events;


-- drop table products
-- drop table categories
-- drop table sales
-- drop table sales_analytics 



/*
Configuration Optimization
*/

/*
Memory Settings
Slide Page: 100
*/
SELECT *
FROM system.settings
where name in (
    'max_memory_usage',
    'max_bytes_before_external_group_by',
    'max_bytes_before_external_sort',
    'join_algorithm',
    'max_memory_usage_for_all_queries',
    'max_threads')

-- SET max_memory_usage = 1073741824
/* 
Query parallelism
Slide Page : 102
*/
SELECT *
FROM system.settings
where name in (
    'max_parallel_replicas',
    'group_by_overflow_mode',
'distributed_aggregation_memory_efficient',
'parallel_view_processing', 'max_threads' , 'max_memory_usage_for_all_queries')


select * from system.metrics
select * from system.events
select * from system.asynchronous_metrics
select * from system.processes


select * from system.query_log
--select * from system.query_thread_log
select * from system.query_views_log
select * from system.trace_log


