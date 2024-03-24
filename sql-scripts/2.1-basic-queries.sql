/*
2.1 Basic Queries
*/

-- Slide Page: 26
CREATE TABLE click_events
(
    `dt` DateTime,
    `event` String,
    `status` Enum8('success' = 1, 'fail' = 2)
)
ENGINE = MergeTree
ORDER BY dt


INSERT INTO click_events SELECT
    (parseDateTimeBestEffortOrNull('12:00') - toIntervalHour(randNormal(0, 3))) - toIntervalDay(number % 30),
    'Click',
    ['fail', 'success'][randBernoulli(0.9) + 1]
FROM numbers(10000000)

SELECT
    dt,
    count(*) AS c,
    bar(c, 0, 100000)
FROM click_events
GROUP BY dt
ORDER BY dt ASC



-- Slide Page: 28
-- Create tables
CREATE TABLE IF NOT EXISTS people
(
    id LowCardinality(FixedString(13)),
    first_name String,
    last_name String,
    dateOfBirth Date,
    laser_id String
)
ENGINE = MergeTree
Primary Key (id, laser_id)
ORDER BY (id,laser_id, dateOfBirth);

/*
1. Basic SELECT:
Retrieve all columns for all records in the arisedb.people table.
*/

SELECT *
FROM people limit 10;

/*
2.Filtering with WHERE Clause:
Retrieve records for people born after a certain date.
*/

SELECT id, first_name, last_name, dateOfBirth
FROM people
WHERE dateOfBirth > '2000-01-01' limit 10;

SELECT count(*)
FROM people
WHERE dateOfBirth > '2000-01-01';

/*
3. Sorting with ORDER BY:

Retrieve records sorted by last_name in ascending order.
*/

SELECT id, first_name, last_name, dateOfBirth
FROM people
ORDER BY last_name DESC limit 10;

/*
4. Limiting Results:
Retrieve the first 10 records from the arisedb.people table.
*/

SELECT *
FROM people
LIMIT 10;


/*
Update and Delete 
Slide Page : 29
*/

Alter table people
Update last_name = 'Zulauf-Wiza'
Where id ='3158190887792'

select * from people
Where id ='3158190887792'

Delete from people Where id ='3158190887792'



