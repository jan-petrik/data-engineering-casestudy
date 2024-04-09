-- define schema to access streaming data
CREATE EXTERNAL SCHEMA kinesis_temus_data
FROM
    KINESIS IAM_ROLE 'arn:aws:iam::533266976778:role/AmazonRedshiftAllCommandsFullAccess';

-- define schemas for processed data
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

-- create materialized view for data from streaming
CREATE MATERIALIZED VIEW bronze.temus_vendor AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,
    refresh_time,
    JSON_PARSE(kinesis_data) as payload
FROM
    kinesis_temus_data."temus-vendor-data-staging";

CREATE MATERIALIZED VIEW bronze.temus_product AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,
    refresh_time,
    JSON_PARSE(kinesis_data) as payload
FROM
    kinesis_temus_data."temus-product-data-staging";