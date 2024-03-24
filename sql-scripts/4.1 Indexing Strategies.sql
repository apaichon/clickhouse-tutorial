/*
Primary Key and Sparse index
Slide Page: 79
*/

-- Create a table that has a compound primary key with key columns UserID and URL:
CREATE TABLE hits_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY (UserID, URL)
ORDER BY (UserID, URL, EventTime)
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;

-- The primary key in the DDL statement above causes the creation of the primary index based on the two specified key columns.
INSERT INTO hits_UserID_URL SELECT
   intHash32(UserID) AS UserID,
   URL,
   EventTime
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz', 'TSV', 'WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16), URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8, FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8, SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8, HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16), RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16, SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8')
WHERE URL != '';

--And optimize the table:
OPTIMIZE TABLE hits_UserID_URL FINAL;

-- We can use the following query to obtain metadata about our table:
SELECT
    part_type,
    path,
    formatReadableQuantity(rows) AS rows,
    formatReadableSize(data_uncompressed_bytes) AS data_uncompressed_bytes,
    formatReadableSize(data_compressed_bytes) AS data_compressed_bytes,
    formatReadableSize(primary_key_bytes_in_memory) AS primary_key_bytes_in_memory,
    marks,
    formatReadableSize(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE (table = 'hits_UserID_URL') AND (active = 1)
FORMAT Vertical;


/* 
Skipping Index
Slide Page: 82
*/

CREATE TABLE skip_table
(
  my_key UInt64,
  my_value UInt64
)
ENGINE MergeTree primary key my_key
SETTINGS index_granularity=8192;


INSERT INTO skip_table SELECT number, intDiv(number,4096) FROM numbers(100000000);

--When executing a simple query that does not use the primary key, all 100 million entries in the my_value column are scanned:

SELECT * FROM skip_table WHERE my_value IN (125, 700)

--Now add a very basic skip index:
ALTER TABLE skip_table ADD INDEX vix my_value TYPE set(100) GRANULARITY 2;

-- Rerun the query with the newly created index:
SELECT * FROM skip_table WHERE my_value IN (125, 700)

/* 
Skipping Index - Min max type
Slide Page: 83
*/

CREATE TABLE orders (
    order_id UInt64,
    customer_id UInt32,
    order_date Date,
    total_amount Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (order_date, order_id);


INSERT INTO orders (order_id, customer_id, order_date, total_amount)
SELECT
    number + 1 AS order_id,
    rand64() % 1000 AS customer_id,
    today() - rand() % 365 AS order_date,
    rand64() % 100000 / 100.0 AS total_amount
FROM numbers(10000000);


SELECT count(*)
FROM orders
WHERE total_amount BETWEEN 700  AND 999;

-- select min(total_amount), max(total_amount) , avg(total_amount) FROM orders


-- drop index idx_total_amount on orders

-- SHOW INDEXES from orders

ALTER TABLE orders ADD INDEX idx_total_amount total_amount type MINMAX GRANULARITY 8192;
ALTER TABLE orders  MATERIALIZE INDEX idx_total_amount ;


/*
SET INDEX
Slide Page: 84
*/

CREATE TABLE products (
    product_id UInt32,
    category_id UInt16,
    name String,
    price Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (product_id, category_id);

INSERT INTO products (product_id, category_id, name, price)
SELECT
    number + 1 AS product_id,
    rand64() % 20 + 1 AS category_id,
    concat('Product ', toString(number + 1)) AS name,
    rand64() % 10000 / 100.0 AS price
FROM numbers(1000000);


select min(category_id), max(category_id) from products

select count(*) from products
where category_id in (5,6)


-- drop index idx_category_id on category_id
ALTER TABLE products ADD INDEX idx_category_id category_id type SET(20) GRANULARITY 8192;

-- drop index idx_category_id on products

-- SHOW INDEXES from products


/*
Bloom Filter Index
Slide Page: 85
*/

CREATE TABLE users (
    user_id UInt64,
    email String,
    city String
) ENGINE = MergeTree()
ORDER BY user_id;


INSERT INTO users (user_id, email, city)
SELECT
    number + 1 AS user_id,
    concat('user', toString(number + 1), '@example.com') AS email,
    arrayElement(
        ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'],
        rand64() % 10
    ) AS city
FROM numbers(1000000);

SELECT count(*)
FROM users
WHERE city IN ('New York', 'Los Angeles', 'Chicago');

Explain indexes=1 SELECT count(*)
FROM users
WHERE city IN ('New York', 'Los Angeles', 'Chicago');


ALTER TABLE users ADD INDEX idx_city_bloom_filter_index city TYPE Bloom_Filter GRANULARITY 8192;
-- ALTER TABLE users ADD INDEX idx_city_bloom_filter_index city TYPE ngrambf_v1(4,1024, 1, 0);

ALTER TABLE users
MATERIALIZE INDEX idx_city_bloom_filter_index

-- drop index idx_city_bloom_filter_index  on users

-- [any_transformations](column_to_index) TYPE ngrambf_v1(4, 1024, 1, 0) 



SHOW INDEXES from users

drop table users

/* 
Partition Keys 
Slide Page: 86
*/
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


SELECT
    partition,
    name,
    rows,
    active
FROM system.parts
WHERE table = 'events'


/* 
Sorting Key 
Slide Page: 87
*/
-- drop table orders
CREATE TABLE orders (
    order_id UInt64,
    customer_id UInt32,
    order_date Date,
    total_amount Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (order_date, order_id);

INSERT INTO orders (order_id, customer_id, order_date, total_amount)
SELECT
    number + 1 AS order_id,
    rand64() % 1000 AS customer_id,
    today() - rand() % 365 AS order_date,
    rand64() % 100000 / 100.0 AS total_amount
FROM numbers(10000000);


SELECT
    partition,
    partition_id,
    name,
    rows,
    path,
    active
FROM system.parts
WHERE table = 'orders'


SELECT
    partition,
    name,
    partition_id
FROM system.parts
WHERE (table = 'orders') AND (partition_id IN (
      SELECT  distinct _partition_id
    FROM orders
    WHERE order_date = '2023-03-24')
    )

   











