"""Let's start by pivoting the events table to get counts for each event_name.

We want to aggregate the number of times each user performed a specific event, 
specified in the event_name column. 
To do this, group by user_id and pivot on event_name to provide a count of every event type in its own column, 
resulting in the schema below. Note that user_id is renamed to user in the target schema."""

-- ANSWER
CREATE OR REPLACE TEMP VIEW events_pivot AS
SELECT * FROM (
  SELECT user_id user, event_name 
  FROM events
) PIVOT ( count(*) FOR event_name IN (
    "cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
    "register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
    "cc_info", "foam", "reviews", "original", "delivery", "premium" ))

%python
# ANSWER
(spark.read.table("events")
    .groupBy("user_id")
    .pivot("event_name")
    .count()
    .withColumnRenamed("user_id", "user")
    .createOrReplaceTempView("events_pivot"))

-- ANSWER
CREATE OR REPLACE TEMP VIEW clickpaths AS
SELECT * 
FROM events_pivot a
JOIN transactions b 
  ON a.user = b.user_id

%python
# ANSWER
from pyspark.sql.functions import col
(spark.read.table("events_pivot")
    .join(spark.table("transactions"), col("events_pivot.user") == col("transactions.user_id"), "inner")
    .createOrReplaceTempView("clickpaths"))

