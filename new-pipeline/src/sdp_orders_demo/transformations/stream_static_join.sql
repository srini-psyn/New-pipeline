-- Stream-static join demo. Assumes orders_bronze exists in this pipeline.
CREATE OR REFRESH MATERIALIZED VIEW customers_dim
AS SELECT * FROM VALUES
  ('C001', 'Asha', 'Chennai'),
  ('C002', 'Ravi', 'Bengaluru'),
  ('C003', 'Meera', 'Hyderabad'),
  ('C004', 'Arun', 'Chennai')
AS customers(customer_id, customer_name, city);

CREATE OR REFRESH STREAMING TABLE orders_enriched
AS
SELECT o.order_id, o.customer_id, c.customer_name, c.city,
       o.order_ts, o.amount, o.status
FROM STREAM(orders_bronze) AS o
LEFT JOIN customers_dim AS c ON o.customer_id = c.customer_id;
