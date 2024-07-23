"""Load Data Lab
In this lab, you will load data into new and existing Delta tables.

Learning Objectives
By the end of this lab, you should be able to:

Create an empty Delta table with a provided schema
Insert records from an existing table into a Delta table
Use a CTAS statement to create a Delta table from files"""


CREATE OR REPLACE TABLE events_raw (
    key BINARY,
    offset LONG,
    partition INTEGER,
    timestamp LONG,
    topic STRING,
    value BINARY
);

INSERT INTO events_raw
SELECT 
    key,
    offset,
    partition,
    timestamp,
    topic,
    value
FROM events_json;


CREATE TABLE item_lookup AS SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/item-lookup`