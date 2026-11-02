-- Lakeflow Declarative Pipelines / SDP teaching demo with edge cases
-- Set pipeline configuration key `source` to the directory containing the JSON files.
-- Example: /Volumes/<catalog>/<schema>/<volume>/sdp_demo/data

-- BRONZE: preserve source records and ingest new JSON files incrementally.
CREATE OR REFRESH STREAMING TABLE orders_bronze
AS
SELECT *
FROM STREAM read_files(
  '${source}',
  format => 'json'
);

-- SILVER: normalize fields and drop records that fail explicit data-quality rules.
-- TRY_CAST / try_to_timestamp turn bad values into NULL so expectations can handle them.
CREATE OR REFRESH STREAMING TABLE orders_silver (
  CONSTRAINT valid_order_id
    EXPECT (order_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id
    EXPECT (customer_id IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_timestamp
    EXPECT (order_ts IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount
    EXPECT (amount IS NOT NULL AND amount >= 0)
    ON VIOLATION DROP ROW,
  CONSTRAINT known_status
    EXPECT (status IN ('COMPLETE', 'PENDING', 'CANCELLED'))
    ON VIOLATION DROP ROW
)
AS
SELECT
  TRY_CAST(order_id AS STRING) AS order_id,
  TRY_CAST(customer_id AS STRING) AS customer_id,
  try_to_timestamp(order_ts) AS order_ts,
  TRY_CAST(amount AS DECIMAL(12,2)) AS amount,
  UPPER(TRIM(status)) AS status
FROM STREAM(orders_bronze);

-- GOLD: summarize only records that pass Silver's expectations.
CREATE OR REFRESH MATERIALIZED VIEW gold_orders_by_date
AS
SELECT
  DATE(order_ts) AS order_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM orders_silver
GROUP BY DATE(order_ts);
