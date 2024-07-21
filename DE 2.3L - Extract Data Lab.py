"""This lab consist of creating a table based in a JSON file

Extract Raw Events From JSON Files
To load this data into Delta properly, we first need to extract the JSON data using the correct schema.
Create an external table against JSON files located at the filepath provided below. Name this table events_json and declare the schema above.

Hint: 
Use a CTAS statement
Use CAST to ensure the data types are correct."""


json_path = f"{DA.paths.kafka_events}"

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS events_json
USING JSON
OPTIONS (path '{json_path}')
AS
SELECT 
    CAST(key AS BINARY) AS key,
    CAST(offset AS BIGINT) AS offset,
    CAST(partition AS INT) AS partition,
    CAST(timestamp AS BIGINT) AS timestamp,
    CAST(topic AS STRING) AS topic,
    CAST(value AS BINARY) AS value
FROM
    json.`{json_path}`;
"""
)
