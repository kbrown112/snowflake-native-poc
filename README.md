# Snowflake Native POC

1. Data ingestion (creates taxi_info table and inserts 2015 and 2016 data)
2. Alter tables (matches schema of 2020 data, inserts several columns)
3. Upload to S3 (preprocess_to_s3.py loads data into S3 bucket--SQS must be set up to recognize unique filepath--and to the stage)
4. Snowpipe takes data from the stage and inserts into taxi_info table)
