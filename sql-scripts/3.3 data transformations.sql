/*
View
Slide Page: 73
*/


Create View vw_bank_transaction
as 
SELECT
    bt.transaction_id as transaction_id,
    bt.transaction_date as transaction_date,
    bt.amount as amount,
    bt.transaction_type as transaction_type,
    b.bank_name as bank_name,
    br.branch_name as branch_name,
    ba.account_number as account_number
FROM
    bank_transactions bt
INNER JOIN
    bank_account ba ON bt.account_id = ba.account_id
INNER JOIN
    branch br ON ba.branch_id = br.branch_id
INNER JOIN
    bank b ON br.bank_id = b.bank_id;

select * from vw_bank_transaction

select * from system.tables where engine = 'View'


/*
Materialized View
Slide Page: 74
*/

-- prepare data table
CREATE TABLE t_transaction_history
(
    id UUID,
    transaction_date DateTime,
    account_id FixedString(10),
    transaction_type Enum('Deposit' =1, 'Withdraw' = 2, 'Transfer' = 3, 'Payment' =4),
    amount Decimal(18,6)
)
ENGINE = MergeTree()
Primary Key (account_id, transaction_date)
Order by (account_id,transaction_date,transaction_type)


-- prepare data
insert into t_transaction_history
(id,  transaction_date,  account_id, transaction_type , amount)

with random as  
(
SELECT
    CAST(
        (RAND() /10000000000) * (max_value - min_value + 1) + min_value AS UInt64
    ) AS random_integer
FROM (
    SELECT 1376788424 AS min_value, 1676788424 AS max_value  from  numbers(100)
)a ),

transform as (
select generateUUIDv4() as id   ,
toDateTime (random_integer) as transaction_date,
 toFixedString(toString(1000000000 + CAST(1000000000  * rand() AS UInt32)),10)  as account_id,
1 + CAST(3 * rand() AS UInt8) % 4 as transaction_type,
100 + CAST(4900 * rand() AS UInt32) % 5000  as amount
from  random)

select  id, transaction_date, account_id, transaction_type,  case when transaction_type > 1 then -1 else 1 end * amount as realAmount 
from transform
;

-- check total data
select count(*) from t_transaction_history


-- Materialized View
-- drop table transaction_summary_by_type_day

-- truncate table transaction_summary_by_type_day
CREATE Table transaction_summary_by_type_day
(
    transaction_day Date,
    transaction_type Enum('Deposit' =1, 'Withdraw' = 2, 'Transfer' = 3, 'Payment' =4),
    min_amount Decimal(18,6),
    max_amount Decimal(18,6),
    avg_amount Decimal(18,6),
    std_dev_amount Decimal(18,6)
)
ENGINE = AggregatingMergeTree
ORDER BY (transaction_type, transaction_day)

CREATE MATERIALIZED VIEW mv_transaction_summary_by_type_day
to transaction_summary_by_type_day

AS 
SELECT
    toDate(transaction_date) AS transaction_day,
    transaction_type,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    avg(amount) AS avg_amount,
    stddevPop(amount) AS std_dev_amount
FROM t_transaction_history
GROUP BY transaction_day, transaction_type;

insert into transaction_summary_by_type_day
( transaction_day,
    transaction_type ,
    min_amount ,
    max_amount ,
    avg_amount ,
    std_dev_amount)
SELECT
    toDate(transaction_date) AS transaction_day,
    transaction_type,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    avg(amount) AS avg_amount,
    stddevPop(amount) AS std_dev_amount
FROM t_transaction_history
GROUP BY transaction_day, transaction_type;


select * from mv_transaction_summary_by_type_day

insert into t_transaction_history
(id,  transaction_date,  account_id, transaction_type , amount)

SELECT
    toDate(transaction_date) AS transaction_day,
    transaction_type,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    avg(amount) AS avg_amount,
    stddevPop(amount) AS std_dev_amount
FROM t_transaction_history
GROUP BY transaction_day, transaction_type;

select * from mv_transaction_summary_by_type_day


/*
Aggregating Table
Slide Page :75
*/

-- Monthly
CREATE Table transaction_summary_by_type_month
(
    transaction_month Date,
    transaction_type Enum('Deposit' =1, 'Withdraw' = 2, 'Transfer' = 3, 'Payment' =4),
    min_amount Decimal(18,6),
    max_amount Decimal(18,6),
    avg_amount Decimal(18,6),
    std_dev_amount Decimal(18,6)
)
ENGINE = AggregatingMergeTree
ORDER BY (transaction_type, transaction_month)

CREATE MATERIALIZED VIEW mv_transaction_summary_by_type_month
to transaction_summary_by_type_month

AS 
SELECT
    toStartOfMonth(transaction_date) AS transaction_month,
    transaction_type,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    avg(amount) AS avg_amount,
    stddevPop(amount) AS std_dev_amount
FROM t_transaction_history
GROUP BY transaction_month, transaction_type;

insert into transaction_summary_by_type_month
( transaction_month,
    transaction_type ,
    min_amount ,
    max_amount ,
    avg_amount ,
    std_dev_amount
)
select * from (
SELECT
    toStartOfMonth(transaction_date) AS transaction_month,
    transaction_type,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    avg(amount) AS avg_amount,
    stddevPop(amount) AS std_dev_amount
FROM t_transaction_history
GROUP BY
    transaction_month,
    transaction_type
)a 
order by transaction_month


 -- Quartery
Select * from (
SELECT
    toStartOfQuarter(transaction_date) AS transaction_quarter,
    transaction_type,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    avg(amount) AS avg_amount,
    stddevPop(amount) AS std_dev_amount
FROM t_transaction_history
GROUP BY
    transaction_quarter,
    transaction_type
)a order by transaction_quarter

-- Yearly
Select * from (
SELECT
    toStartOfYear(transaction_date) AS transaction_year,
    transaction_type,
    min(amount) AS min_amount,
    max(amount) AS max_amount,
    avg(amount) AS avg_amount,
    stddevPop(amount) AS std_dev_amount
FROM t_transaction_history
GROUP BY
    transaction_year,
    transaction_type
)a order by transaction_year


/*
-- Create User Define Function
Slide Page: 76
*/

CREATE FUNCTION date_to_month_name AS (input) -> 
if(input= 1, 'January', 
if(input=2, 'Febuary', 
if(input=3, 'March', 
if(input=4, 'April', 
if(input=5, 'May', 
if(input=6, 'June', 
if(input=7, 'July', 
if(input=8, 'August', 
if(input=9, 'September', 
if(input=10, 'October', 
if(input=11, 'November', 
if(input=12, 'December', 'Invalid'
))))))))))));

select  date_to_month_name(month(transaction_month)) as month_name, *
from mv_transaction_summary_by_type_month



/*
Integrate with python
Slide page : 77
*/
SELECT test_function_python(toUInt64(2));


