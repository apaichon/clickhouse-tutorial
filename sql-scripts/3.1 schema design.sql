
/*
Low Cardinality Types
Slide Page: 46
*/

CREATE TABLE lc_t
(
    id UInt16,
    strings LowCardinality(String)
)
Engine = MergeTree
order by id 

/*
Enumerations 
Slide Page : 47
*/

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

insert into 
t_transaction_history
(id, account_id, transaction_type , amount)


select generateUUIDv4() as id   ,
 toFixedString(toString(1000000000 + CAST(1000000000  * rand() AS UInt32)),10)  as account_id,
1 + CAST(3 * rand() AS UInt8) % 4 as transaction_type,
100 + CAST(4900 * rand() AS Int32) % 5000  as amount
from  numbers(100)

select * from t_transaction_history limit 100


/*
Decimal Type Precision
Slide Page : 48
*/
select 1000 /  0.787564 , 1000 * 0.787564, 1000/0.78 , 1000 * 0.78


/*
DateTime Type
Slide Page: 49
*/

create table test_order_date
(

    ds String
) engine = MergeTree()
Primary Key(ds)

insert into test_order_date
values
('01/03/2024'),('12/12/2023')

select * from test_order_date order by ds

/*
String Type, 
Slide Page : 50
*/

CREATE TABLE my_table (
    id UInt32,
    phone_number FixedString(12) CODEC(ZSTD), 
    CONSTRAINT check_phone_number CHECK phone_number REGEXP '^\\d{3}-\\d{3}-\\d{4}$'
) ENGINE = MergeTree
PRIMARY KEY (id);

Insert into my_table
(id, phone_number)
values
(1, '092-475-9999')


/* Special Types
Slide Page : 53
*/
CREATE TABLE player_data (
    player_id UUID,
    username String,
    team Enum('Mystic' = 1, 'Valor' = 2, 'Instinct' = 3),
    level UInt8,
    pokemon_owned Array(String),
    badges Tuple(UInt8, UInt8, UInt8),
    items Map(String, UInt16),
    achievements JSON,
    location Nested (
        latitude Float64,
        longitude Float64
    ),
    last_active DateTime
) ENGINE = MergeTree
PRIMARY KEY (player_id);

-- select * from system.settings where name = 'allow_experimental_object_type'
--Set allow_experimental_object_type = 1 


/*
 ReplacingMergeTree Engine
 Slide Page : 56
 */
CREATE TABLE BankAccount (
    account_id FixedString(10),
    account_holder String,
    balance Decimal(18, 2),
    last_updated DateTime,
    PRIMARY KEY account_id
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (account_id);
-- drop table BankAccount
INSERT INTO BankAccount 
(
    account_id,
    account_holder,
    balance,
    last_updated
)
VALUES (
        'A001',
        'John Doe',
        5000.00,
        '2023-05-01 10:00:00'
    ),
    (
        'B002',
        'Jane Smith',
        2500.75,
        '2023-05-01 11:30:00'
    ),
    (
        'C003',
        'Michael Johnson',
        7800.25,
        '2023-05-01 09:15:00'
    );

select *
from BankAccount -- Updating the balance for account 'A001'
INSERT INTO BankAccount (
        account_id,
        account_holder,
        balance,
        last_updated
    )
VALUES (
        'A001',
        'John Doe',
        6000.00,
        '2023-05-02 14:45:00'
    );

select *
from BankAccount -- Inserting a new account
INSERT INTO BankAccount (
        account_id,
        account_holder,
        balance,
        last_updated
    )
VALUES (
        'D004',
        'Emily Davis',
        3200.50,
        '2023-05-02 16:20:00'
    );


select *
from BankAccount final;


INSERT INTO BankAccount (
        account_id,
        account_holder,
        balance,
        last_updated
    )
VALUES (
        'A001',
        'John Doe',
        5000.00,
        '2023-05-01 10:00:00'
    );

select *
from BankAccount final OPTIMIZE TABLE BankAccount FINAL DEDUPLICATE;

/*
 CollapsingMergeTree Engine
 Slide Page: 57
 drop table AccountBalance
 */
CREATE TABLE AccountBalance (
    account_id String,
    account_holder String,
    last_transaction_time DateTime,
    amount Decimal(18, 2),
    sign Int8 default 1,
    PRIMARY KEY (account_id, last_transaction_time)
) ENGINE = CollapsingMergeTree(sign) PARTITION BY toYYYYMM(last_transaction_time)
ORDER BY (account_id, last_transaction_time);

INSERT INTO AccountBalance (
        account_id,
        account_holder,
        last_transaction_time,
        amount
    )
VALUES (
        'A001',
        'John Doe',
        '2023-05-01 10:00:00',
        500.00
    ),
    (
        'B002',
        'Jane Smith',
        '2023-05-01 09:15:00',
        1000.00
    ),
    (
        'C003',
        'Michael Johnson',
        '2023-05-02 11:10:00',
        2500.00
    );


select *
from AccountBalance
INSERT INTO AccountBalance (
        account_id,
        account_holder,
        last_transaction_time,
        amount,
        sign
    )
VALUES (
        'A001',
        'John Doe',
        '2023-05-01 15:45:00',
        150.00,
        1
    ),
    (
        'B002',
        'Jane Smith',
        '2023-05-01 18:20:00',
        300.00,
        1
    ),
    (
        'C003',
        'Michael Johnson',
        '2023-05-02 14:30:00',
        1200.00,
        1
    ),
    (
        'A001',
        'John Doe',
        '2023-05-01 10:00:00',
        500.00,
        -1
    ),
    (
        'B002',
        'Jane Smith',
        '2023-05-01 09:15:00',
        1000.00,
        -1
    ),
    (
        'C003',
        'Michael Johnson',
        '2023-05-02 11:10:00',
        2500.00,
        -1
    );

select *
from AccountBalance final
select account_id,
    last_transaction_time,
    sum(sign)
from AccountBalance
GROUP BY account_id,
    last_transaction_time ;
    
OPTIMIZE TABLE AccountBalance FINAL DEDUPLICATE;


/*
SummingMergeTree
Slide Page: 58
drop table transaction_history
 */

 CREATE TABLE IF NOT EXISTS transaction_history (
    transaction_id UUID,
    transaction_type Enum('deposit' =1, 'withdrawal'=2, 'transfer' =3, 'payment' =4),
    account_id UInt64,
    ref_num UUID,
    amount Float64,
    transaction_date DateTime DEFAULT now(),
    created_at DateTime DEFAULT now(),
    created_by UInt64,
    remarks Nullable(String),
    status_id Nullable(UInt8)
) ENGINE = SummingMergeTree
Primary Key (account_id,ref_num)
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (account_id, ref_num, transaction_date, transaction_type);

-- Insert sample data into TransactionHistory table
INSERT INTO transaction_history (transaction_id, transaction_type, account_id, ref_num, amount, transaction_date, created_at, created_by, remarks, status_id)
VALUES
    ('00000000-0000-0000-0000-000000000001', 'deposit', 1001, '11111111-1111-1111-1111-111111111101', 1000.00, '2024-02-01 12:00:00', '2024-02-01 12:00:00', 1, 'Initial deposit', 1),
    ('00000000-0000-0000-0000-000000000002', 'withdrawal', 1001, '11111111-1111-1111-1111-111111111102', -500.00, '2024-02-05 15:30:00', '2024-02-05 15:30:00', 2, 'ATM withdrawal', 1),
    ('00000000-0000-0000-0000-000000000003', 'transfer', 1001, '11111111-1111-1111-1111-111111111103', -200.00, '2024-02-10 09:45:00', '2024-02-10 09:45:00', 1, 'Transfer to savings account', 1),
    ('00000000-0000-0000-0000-000000000004', 'payment', 1001, '11111111-1111-1111-1111-111111111104', -300.00, '2024-02-15 14:20:00', '2024-02-15 14:20:00', 3, 'Credit card payment', 1),
    ('00000000-0000-0000-0000-000000000005', 'deposit', 1002, '11111111-1111-1111-1111-111111111105', 1500.00, '2024-02-02 10:00:00', '2024-02-02 10:00:00', 2, 'Salary credit', 1),
    ('00000000-0000-0000-0000-000000000006', 'withdrawal', 1002, '11111111-1111-1111-1111-111111111106', -700.00, '2024-02-06 11:45:00', '2024-02-06 11:45:00', 1, 'Grocery shopping', 1),
    ('00000000-0000-0000-0000-000000000007', 'transfer', 1002, '11111111-1111-1111-1111-111111111107', -300.00, '2024-02-11 16:00:00', '2024-02-11 16:00:00', 3, 'Transfer to investment account', 1),
    ('00000000-0000-0000-0000-000000000008', 'payment', 1002, '11111111-1111-1111-1111-111111111108', -500.00, '2024-02-20 08:30:00', '2024-02-20 08:30:00', 2, 'Utility bill payment', 1),
    ('00000000-0000-0000-0000-000000000009', 'deposit', 1001, '11111111-1111-1111-1111-111111111109', 900.00, '2024-02-21 12:00:00', '2024-02-21 12:00:00', 1, 'Deposit', 1),
    ('00000000-0000-0000-0000-000000000010', 'deposit', 1002, '11111111-1111-1111-1111-111111111110', 500.00, '2024-02-21 12:00:00', '2024-02-21 12:00:00', 2, 'Deposit', 1);

select account_id , sum(amount) as totalAccount from transaction_history 
group by account_id;


INSERT INTO transaction_history (transaction_id, transaction_type, account_id, ref_num, amount, transaction_date, created_at, created_by, remarks, status_id)
VALUES
    ('00000000-0000-0000-0000-000000000010', 'deposit', 1002, '11111111-1111-1111-1111-111111111110', 500.00, '2024-02-21 12:00:00', '2024-02-21 12:00:00', 2, 'Deposit', 1);

select * from transaction_history final;

OPTIMIZE TABLE transaction_history FINAL DEDUPLICATE;


/*
VersionedCollapsingMergeTree
Slide Page: 59
*/

CREATE TABLE IF NOT EXISTS user_account_states (
    user_id UInt64,
    created_at DateTime default now(),
    created_by UInt64,
    remarks Nullable(FixedString(255)),
    status_id Enum('Inactive' =0, 'Active' = 1, 'Banned' = 2, 'Locked' =3, 'Unscription' =4),
    sign Int8,
    version UInt32
) ENGINE = VersionedCollapsingMergeTree(sign, version)
Primary Key (user_id)
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, created_at);

-- truncate table user_account_states

INSERT INTO user_account_states ( user_id,created_by, remarks,status_id, sign, version)
VALUES
    (1001,  1001,'Register', 0,  1, 1),
    (1002, 1002, 'Register', 0, 1, 1);

INSERT INTO user_account_states ( user_id,created_by, remarks,status_id, sign, version)
VALUES
    (1001, 1001, 'Active',1 ,1, 2);


INSERT INTO user_account_states ( user_id,created_by, created_at, remarks,status_id, sign, version)
VALUES
    (1001, 1001, '<datetime>', 'Register',0 ,-1, 1);

    select * from user_account_states final
    order by user_id, version desc;


    OPTIMIZE TABLE user_account_states  FINAL DEDUPLICATE;


/*
Aggregate Merge Tree
Slide Page: 60
*/

CREATE TABLE IF NOT EXISTS transaction_analytics (
    transaction_date Date,
    transaction_type Enum('Deposit' =1, 'Withdraw' = 2, 'Transfer' = 3, 'Payment' =4),
    account_id UInt64,
    _count UInt64,
    _sum Float64,
    _avg Float64,
    _min Float64,
    _max Float64,
    _peak_time DateTime
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (transaction_date, transaction_type, account_id);


INSERT INTO transaction_analytics

SELECT
    transaction_date,
    transaction_type,
    account_id,
    count() AS Count,
    sum(amount) AS Sum,
    avg(amount) AS Avg,
    min(amount) AS Min,
    max(amount) AS Max,
    argMax(transaction_date, amount) AS PeakTime
FROM t_transaction_history
GROUP BY
    transaction_date,
    transaction_type,
    account_id;

/*
GraphiteMergeTree
Slide Page: 61
*/

-- drop table transaction_status_monitoring
CREATE TABLE IF NOT EXISTS transaction_status_monitoring (
    event_date Date,
    transaction_type String,
    account_id UInt64,
    success Float64,
    event_time DateTime,
    version UInt16
) ENGINE = GraphiteMergeTree('graphite_rollup')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_time, transaction_type );

INSERT INTO  transaction_status_monitoring (event_date, transaction_type, account_id, success, event_time,version )
VALUES
    ('2023-03-13', 'Deposit', 123456, 1, '2023-03-13 10:00:00',1),
    ('2023-03-13', 'Withdrawal', 123456, 1, '2023-03-13 11:00:00',1),
    ('2023-03-13', 'Transfer', 789012, 1, '2023-03-13 12:50:00',1),
    ('2023-03-13', 'Payment', 123456, 1, '2023-03-13 13:00:00',1);

INSERT INTO  transaction_status_monitoring (event_date, transaction_type, account_id, success, event_time,version )
VALUES
   ('2023-03-13', 'Transfer', 123456, 0, '2023-03-13 13:15:00',1);


select * from transaction_status_monitoring;

/*
Integrate to S3
Slide Page: 63
*/

SELECT *
FROM s3(
   'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv',
   'CSVWithNames'
)
LIMIT 5;

/*
Logs
*/

/* Stripe Log 
Slide Page: 65
*/

CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog

INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message');
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message');

SELECT * FROM stripe_log_table

/* Log 
    Slide Page: 65
*/

CREATE TABLE logs_example (
    timestamp DateTime,
    message String
) ENGINE = Log;

INSERT INTO logs_example (timestamp, message)
VALUES ('2024-03-15 12:00:00', 'Log entry 1'),
       ('2024-03-15 12:05:00', 'Log entry 2'),
       ('2024-03-15 12:10:00', 'Log entry 3');

SELECT *
FROM logs_example

/* Tiny Log 
 Slide Page: 65
*/

CREATE TABLE example_table (
    timestamp DateTime,
    value Float64
) ENGINE = TinyLog

INSERT INTO example_table (timestamp, value)
VALUES 
    ('2024-03-01 12:00:00', 10.5),
    ('2024-03-02 09:30:00', 15.2),
    ('2024-03-03 14:45:00', 20.0),
    ('2024-03-04 08:00:00', 18.7),
    ('2024-03-05 16:20:00', 25.3),
    ('2024-03-06 11:10:00', 30.1),
    ('2024-03-07 13:00:00', 22.6),
    ('2024-03-08 10:45:00', 28.9),
    ('2024-03-09 09:15:00', 35.7),
    ('2024-03-10 15:30:00', 40.2);

select * from example_table


/* Normalization 
Slide Page: 67
*/

-- Bank table
CREATE TABLE IF NOT EXISTS bank (
    bank_id UInt32,
    bank_name String,
    bank_address String,
    phone_number String
) ENGINE = MergeTree()
ORDER BY bank_id;

-- Branch table
CREATE TABLE IF NOT EXISTS branch (
    branch_id UInt32,
    branch_name String,
    branch_address String,
    bank_id UInt32
) ENGINE = MergeTree()
ORDER BY branch_id;

-- BankAccount table
CREATE TABLE IF NOT EXISTS bank_account (
    account_id UInt32,
    account_number String,
    account_type String,
    balance Float64,
    branch_id UInt32
) ENGINE = MergeTree()
ORDER BY account_id;

-- BankTransaction table
CREATE TABLE IF NOT EXISTS bank_transactions (
    transaction_id UInt32,
    transaction_date DateTime,
    amount Float64,
    transaction_type Enum8('deposit' = 1, 'withdrawal' = 2, 'transfer' = 3),
    account_id UInt32
) ENGINE = MergeTree()
ORDER BY (account_id, transaction_id);


-- Insert sample data into the Bank table
INSERT INTO bank (bank_id, bank_name, bank_address, phone_number)
VALUES 
    (1, 'Bank A', 'Address A', '123-456-7890'),
    (2, 'Bank B', 'Address B', '987-654-3210');

-- Insert sample data into the Branch table
INSERT INTO branch (branch_id, branch_name, branch_address, bank_id)
VALUES 
    (1, 'Branch X', 'Address X', 1),
    (2, 'Branch Y', 'Address Y', 1),
    (3, 'Branch Z', 'Address Z', 2);

-- Insert sample data into the BankAccount table
INSERT INTO bank_account (account_id, account_number, account_type, balance, branch_id)
VALUES 
    (1, '10001', 'Savings', 5000.00, 1),
    (2, '10002', 'Checking', 2500.00, 1),
    (3, '20001', 'Savings', 7500.00, 2),
    (4, '20002', 'Checking', 3000.00, 3);

-- Insert sample data into the BankTransaction table
INSERT INTO bank_transactions (transaction_id, transaction_date, amount, transaction_type, account_id)
VALUES 
    (1, '2024-03-01 12:00:00', 1000.00, 'deposit', 1),
    (2, '2024-03-02 09:30:00', 200.00, 'withdrawal', 2),
    (3, '2024-03-03 14:45:00', 1500.00, 'deposit', 3),
    (4, '2024-03-04 08:00:00', 500.00, 'withdrawal', 4);


SELECT
    bt.transaction_id,
    bt.transaction_date,
    bt.amount,
    bt.transaction_type,
    b.bank_name,
    br.branch_name,
    ba.account_number
FROM
    bank_transactions bt
INNER JOIN
    bank_account ba ON bt.account_id = ba.account_id
INNER JOIN
    branch br ON ba.branch_id = br.branch_id
INNER JOIN
    bank b ON br.bank_id = b.bank_id;


/* 
De-normalization 
Slide Page: 68

*/

CREATE TABLE IF NOT EXISTS customer_orders_denormalized (
    order_date Date,
    customer_name String,
    email String,
    product_name Array(String),
    unit Array(String),
    qty Array(UInt32),
    unit_price Array(Float64),
    total_price Float64
) ENGINE = MergeTree()
ORDER BY order_date;


INSERT INTO customer_orders_denormalized (order_date, customer_name, email, product_name, unit, qty, unit_price, total_price)
VALUES 
    ('2024-03-01', 'John Doe', 'john.doe@example.com', ['Product A', 'Product B'], ['pcs', 'pcs'], [2, 3], [10.5, 15.2], 67.1),
    ('2024-03-02', 'Alice Smith', 'alice.smith@example.com', ['Product C'], ['pcs'], [1], [20.0], 20.0),
    ('2024-03-03', 'Bob Johnson', 'bob.johnson@example.com', ['Product D', 'Product E'], ['pcs', 'pcs'], [1, 2], [8.0, 12.5], 29.0);

select * from customer_orders_denormalized


CREATE TABLE IF NOT EXISTS invoice_denormalized (
    invoice_date Date,
    invoice_no String,
    due_date Date,
    customer_name String,
    product_name Array(String),
    unit Array(String),
    qty Array(UInt32),
    unit_price Array(Float64),
    total_price Float64
) ENGINE = MergeTree()
ORDER BY invoice_date;


INSERT INTO invoice_denormalized (invoice_date, invoice_no, due_date, customer_name, product_name, unit, qty, unit_price, total_price)
VALUES 
    ('2024-03-01', 'INV001', '2024-03-31', 'John Doe', ['Product A', 'Product B'], ['pcs', 'pcs'], [2, 3], [10.5, 15.2], 67.1),
    ('2024-03-02', 'INV002', '2024-03-30', 'Alice Smith', ['Product C'], ['pcs'], [1], [20.0], 20.0),
    ('2024-03-03', 'INV003', '2024-03-29', 'Bob Johnson', ['Product D', 'Product E'], ['pcs', 'pcs'], [1, 2], [8.0, 12.5], 29.0);



-- De-normalize Data.
CREATE TABLE products (
    product_id UInt32,
    product_name String,
    category_id UInt16
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE categories (
    category_id UInt16,
    category_name String
) ENGINE = MergeTree()
ORDER BY category_id;

CREATE TABLE sales (
    sale_id UInt64,
    product_id UInt32,
    sale_date Date,
    quantity UInt16,
    price Decimal(10, 2)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY (sale_date, sale_id);

CREATE TABLE sales_analytics (
    sale_id UInt64,
    sale_date Date,
    product_id UInt32,
    product_name String,
    category_id UInt16,
    category_name String,
    quantity UInt16,
    price Decimal(10, 2),
    revenue Decimal(16, 2)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY (sale_date, sale_id);

INSERT INTO categories (category_id, category_name)
SELECT
    number + 1 AS category_id,
    'Category ' || toString(number + 1) AS category_name
FROM numbers(10);

INSERT INTO products (product_id, product_name, category_id)
SELECT
    number + 1 AS product_id,
    'Product ' || toString(number + 1) AS product_name,
    rand64() % 10 + 1 AS category_id
FROM numbers(1000);

INSERT INTO sales (sale_id, product_id, sale_date, quantity, price)
SELECT
    number + 1 AS sale_id,
    rand64() % 1000 + 1 AS product_id,
    today() - rand() % 365 AS sale_date,
    rand64() % 10 + 1 AS quantity,
    rand64() % 10000 / 100.0 AS price
FROM numbers(10000000);

select * from (
SELECT 
    toYear(s.sale_date) as by_year,
    p.product_name as product_name,
    c.category_name as category_name,
    sum(s.quantity) as quantity,
    sum(s.price) as total_price,
    sum(s.quantity) * sum(s.price) AS revenue
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
group by by_year, product_name, category_name
)a
order by by_year desc, revenue desc


INSERT INTO sales_analytics (
    sale_id,
    sale_date,
    product_id,
    product_name,
    category_id,
    category_name,
    quantity,
    price,
    revenue
)
SELECT
    s.sale_id,
    s.sale_date,
    s.product_id,
    p.product_name,
    p.category_id,
    c.category_name,
    s.quantity,
    s.price,
    s.quantity * s.price AS revenue
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id;


-- select  * FROM sales_analytics limit 10 
SELECT 
    toYear(sale_date) as by_year,
    product_name ,
    category_name,
sum(quantity) as total_quantity,
    sum(price) as total_price,
    sum(quantity) * sum(price) AS revenue
FROM sales_analytics
group by by_year, product_name, category_name