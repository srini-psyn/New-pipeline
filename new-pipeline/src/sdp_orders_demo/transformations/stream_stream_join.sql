-- Stream-stream join demo. Configure impressions_source and clicks_source.
CREATE OR REFRESH STREAMING TABLE clicks
AS
SELECT
  CAST(ad_id AS STRING) AS ad_id,
  CAST(user_id AS STRING) AS user_id,
  try_to_timestamp(click_time) AS click_time
FROM STREAM read_files(
  :clicks_source,
  format => 'json',
  schema => 'ad_id STRING, user_id STRING, click_time STRING'
);

CREATE OR REFRESH STREAMING TABLE impressions
AS
SELECT
  CAST(ad_id AS STRING) AS ad_id,
  CAST(user_id AS STRING) AS user_id,
  try_to_timestamp(impression_time) AS impression_time
FROM STREAM read_files(
  :impressions_source,
  format => 'json',
  schema => 'ad_id STRING, user_id STRING, impression_time STRING'
);


CREATE OR REFRESH STREAMING TABLE impression_click_matches
AS
SELECT
  imp.ad_id,
  imp.user_id AS impression_user_id,
  imp.impression_time,
  clk.click_time
FROM STREAM(impressions)
  WATERMARK impression_time
  DELAY OF INTERVAL 3 MINUTES AS imp
INNER JOIN STREAM(clicks)
  WATERMARK click_time
  DELAY OF INTERVAL 3 MINUTES AS clk
ON imp.ad_id = clk.ad_id
AND imp.user_id = clk.user_id
AND clk.click_time BETWEEN imp.impression_time
  AND imp.impression_time + INTERVAL 3 MINUTES;


