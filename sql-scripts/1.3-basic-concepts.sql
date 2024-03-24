/* 1.3 Basic Concept
*/

-- Create database : Slide Page : 12
CREATE DATABASE IF NOT EXISTS testdb;

-- Create tables : Slide Page : 13
CREATE TABLE IF NOT EXISTS testdb.people
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


-- Show Databases;

-- select * from testdb.people limit 10


-- Partition : Slide page : 20
Create table if NOT EXISTS payment
(
    payment_id UInt64,
    amount Float64,
    payment_date DateTime,
    customer_id UInt32
)
ENGINE = MergeTree
Partition By toYYYYMM(payment_date)
order by (payment_date, payment_id, customer_id )
