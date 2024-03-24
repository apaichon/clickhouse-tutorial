/* 
2.2 Advanced Queries
*/

/*drop table customers
drop table orders 
drop table products
drop table  order_items
*/


-- Join Operations, Slide Page: 31

CREATE TABLE IF NOT EXISTS customers (
    customer_id UInt32,
    customer_name String,
    email String,
    date_of_birth Date
) ENGINE = MergeTree() PRIMARY KEY (customer_id);

INSERT INTO customers (customer_id, customer_name, email,date_of_birth)
VALUES (1, 'John Doe', 'john@example.com', '1990-01-01'),
    (2, 'Jane Smith', 'jane@example.com', '2001-05-09'),
    (3, 'Bob Johnson', 'bob@example.com', '1979-11-13');

CREATE TABLE IF NOT EXISTS orders (
    order_id UInt32,
    customer_id UInt32,
    order_date DateTime,
    total_amount Float64
) ENGINE = MergeTree() PRIMARY KEY (order_id, customer_id)
Order by (order_id, customer_id, order_date);

INSERT INTO orders (order_id, customer_id, order_date, total_amount)
VALUES (101, 1, '2023-02-15', 150.50),
    (102, 2, '2023-02-16', 220.75),
    (103, 3, '2023-02-17', 120.00);

CREATE TABLE IF NOT EXISTS products (
    product_id UInt32,
    product_name String,
    price Float64
) ENGINE = MergeTree() PRIMARY KEY (product_id, product_name);

INSERT INTO products (product_id, product_name, price)
VALUES (501, 'Laptop', 800.00),
    (502, 'Smartphone', 500.00),
    (503, 'Headphones', 80.00);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity UInt32,
    subtotal Float64
) ENGINE = MergeTree() PRIMARY KEY (order_item_id, order_id)
Order by (order_item_id, order_id, product_id);

INSERT INTO order_items (
        order_item_id,
        order_id,
        product_id,
        quantity,
        subtotal
    )
VALUES (1001, 101, 501, 2, 1600.00),
    (1002, 101, 502, 1, 500.00),
    (1003, 102, 503, 3, 240.00);

-- Inner join
SELECT c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_date,
    p.product_name,
    oi.quantity,
    oi.subtotal
FROM customers AS c
    INNER JOIN orders AS o ON c.customer_id = o.customer_id
    INNER JOIN order_items AS oi ON o.order_id = oi.order_id
    INNER JOIN products AS p ON oi.product_id = p.product_id;


/*
 Sub Queries  Page: 32
 */

SELECT
    customer_id,
    customer_name,
    (SELECT count() FROM orders WHERE orders.customer_id = customer_id) AS total_orders
FROM customers 

SELECT
    c.customer_name,
    o.order_date,
    o.total_amount
FROM
    orders o
    left join customers c 
    on o.customer_id = c.customer_id
WHERE
    total_amount > (SELECT AVG(total_amount) FROM orders);


/* Aggregate Queries Page: 33 */

select 
    count(*) as totalItems,
    min(total_amount) as minAmount, 
    avg(total_amount) as avgAmount,
    max(total_amount) as maxAmount,
    sum(total_amount) as  totalAmount 
    from orders

SELECT
    p.product_id,
    p.product_name,
    sum(oi.quantity) AS total_quantity_sold
FROM products AS p
LEFT JOIN order_items AS oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_quantity_sold DESC
LIMIT 10; 

/* With Clause Page: 34 */


INSERT INTO customers (customer_id, customer_name, email)
SELECT
    number AS customer_id,
    concat('Customer ', toString(number)) AS customer_name,
    concat('customer', toString(number), '@example.com') AS email
FROM numbers(1, 1000); -- Generate 1000 customers

-- Generate sampling data for the products table
INSERT INTO products (product_id, product_name, price)
SELECT
    number AS product_id,
    concat('Product ', toString(number)) AS product_name,
    floor(rand() * 1000 + 1) AS price
FROM numbers(1, 500); -- Generate 500 products

-- Generate sampling data for the orders table
INSERT INTO orders (order_id, customer_id, order_date, total_amount)
SELECT
    number AS order_id,
    (number % 1000) + 1 AS customer_id,
    now() AS order_date,
    floor(rand() * 1000 + 1) AS total_amount
FROM numbers(1, 2000); -- Generate 2000 orders

-- Generate sampling data for the order_items table
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, subtotal)
SELECT
    number AS order_item_id,
    (number % 2000) + 1 AS order_id,
    (number % 500) + 1 AS product_id,
    floor(rand() * 10 + 1) AS quantity,
    floor(rand() * 100 + 1) AS subtotal
FROM numbers(1, 5000); -- Generate 5000 order items


WITH
    -- Best Selling Products
    best_selling_products AS (
        SELECT
            p.product_id,
            p.product_name,
            sum(oi.quantity) AS total_quantity_sold
        FROM products AS p
        LEFT JOIN order_items AS oi ON p.product_id = oi.product_id
        GROUP BY p.product_id, p.product_name
        ORDER BY total_quantity_sold DESC
        LIMIT 1
    ),
    -- Worst Performing Products (Least Sold)
    worst_performing_products AS (
        SELECT
            p.product_id,
            p.product_name,
            sum(oi.quantity) AS total_quantity_sold
        FROM products AS p
        LEFT JOIN order_items AS oi ON p.product_id = oi.product_id
        GROUP BY p.product_id, p.product_name
        ORDER BY total_quantity_sold ASC
        LIMIT 1
    ),
    -- Average Age of Best Customer from Top 10
    avg_age_best_customer AS (
        select avg(age) as avg_age from ( 
        select  c.customer_id,  oi.subtotal , toYear(now()) - toYear(c.date_of_birth) as age from customers c 
        inner join orders o on c.customer_id = o.customer_id
        inner join order_items oi on o.order_id = oi.order_id
        order by subtotal desc
        limit 10)a
    ),
    -- Best Hour Time to Sales
    best_hour_sales AS (
        SELECT
            toHour(o.order_date) AS hour,
            count() AS total_orders
        FROM orders AS o
        GROUP BY hour
        ORDER BY total_orders DESC
        LIMIT 1
    )
-- Combine the results

    select  
        b.product_name as best_seller_product, 
        w.product_name as worst_performing_product ,
        age_c.avg_age as best_customer_avg_age,
        bhs.hour as best_hour_sales
    from best_selling_products b, 
        worst_performing_products w,
        avg_age_best_customer age_c,
        best_hour_sales bhs


