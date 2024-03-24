
/*
Sparse Index
Slide Page: 37
*/    
    
SELECT * FROM mergeTreeIndex(currentDatabase(), people, with_marks = true);


/*
Skipping Index
Slide Page: 38
*/

CREATE TABLE IF NOT EXISTS sample_table
(
    column1 Int32,
    column2 String,
    column3 Float64
) ENGINE = MergeTree
ORDER BY column1;

-- Create a Data Skipping Index on column1
CREATE INDEX idx_column1_skip
    ON sample_table(column1)
    TYPE minmax
    GRANULARITY 1;

insert into sample_table
values
(1, 'Hello 1', 99), (2, 'Hello 2',  5000),(3, 'Hello 3', 100), (4, 'Hello 4',  5009)

SELECT * FROM mergeTreeIndex(currentDatabase(), sample_table);

explain indexes=1 select * from sample_table
where column1 BETWEEN 2 and 3


/*
ANN Index
Slide Page: 39
*/

CREATE TABLE IF NOT EXISTS my_table (
    id UInt32,
    vector Array(Float32)
) ENGINE = MergeTree
Primary Key (id)

-- Insert sample data
INSERT INTO my_table (id, vector)
VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [2.0, 3.0, 4.0]),
    (3, [3.0, 4.0, 5.0]),
    (4, [4.0, 5.0, 6.0]);

-- Create the Usearcn index
ALTER TABLE my_table ADD INDEX vector_ann_index vector type usearch('cosineDistance', 'f32')


INSERT INTO my_table (id, vector)
SELECT
    number AS id,
    arrayMap(x -> rand() * 10, range(3)) AS vector
FROM numbers(1000000);

-- SHOW PROCESSLIST;
-- KILL QUERY WHERE query_id = '749082f7-f2f5-46ab-a178-f6e85dfa72b5';

SELECT
    id,
    dotProduct(vector, [2.0, 3.0, 4.0]) / (length(vector) * length([2.0, 3.0, 4.0])) AS cosine_similarity
FROM my_table
ORDER BY cosine_similarity DESC limit 100;

SELECT id, vector
FROM my_table
WHERE dotProduct(vector, [1.0, 2.0, 3.0]) > 0.9
ORDER BY dotProduct(vector, [1.0, 2.0, 3.0]) DESC
LIMIT 10;


/*
Full Text search 
Slide Page: 40
*/
-- Create a table
CREATE TABLE IF NOT EXISTS my_table_text (
    id UInt32,
    text String
) ENGINE = MergeTree
PRIMARY KEY (id);

-- Add a Full-Text Search index
ALTER TABLE my_table_text ADD INDEX fulltext_index(text) type inverted(3);

select * from system.settings
where name ='allow_experimental_inverted_index'
-- Insert sample data
INSERT INTO my_table_text (id, text)
VALUES
    (1, 'The quick brown fox jumps over the lazy dog.'),
    (2, 'ClickHouse is a fast and scalable analytical database.'),
    (3, 'Full-Text Search indexes improve text search performance.');

-- Perform a Full-Text Search query

SELECT * from my_table_text WHERE text == 'Search indexes';
SELECT * from my_table_text WHERE text IN ('Search', 'indexes');
SELECT * from my_table_text WHERE text LIKE '%Search%';
SELECT * from my_table_text WHERE multiSearchAny(text, ['Search', 'indexes']);
SELECT * from my_table_text WHERE hasToken(text, 'Search');



/*
Trace Log
Slide Page: 41
*/

Select * from system.trace_log


/*
EXPLAIN 
Slide Page: 42
*/
EXPLAIN AST  SELECT c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_date,
    p.product_name,
    oi.quantity,
    oi.subtotal
FROM customers AS c
    INNER JOIN orders AS o ON c.customer_id = o.customer_id
    INNER JOIN order_items AS oi ON o.order_id = oi.order_id
    INNER JOIN products AS p ON oi.product_id = p.product_id
    FORMAT JSON;










