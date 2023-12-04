--Step 1 (Creating Schema)
Create schema if not exists demo_aws_s3.trino_iceberg_edu 
    with (location = 's3://yusuf-cattaneo-bootcamp-nov2022/trino_iceberg_edu/');

USE demo_aws_s3.trino_iceberg_edu;

drop table cdc_accounts;
CREATE TABLE cdc_accounts (
    account_id bigint,
    balance decimal(16,2),
    tid bigint,
    last_updated TIMESTAMP(6) WITH TIME ZONE)
WITH (
  partitioning = ARRAY['bucket(account_id, 128)'],
  format='parquet',
  type = 'ICEBERG'
); 

-- create schema onsite_galaxy_postgres.cdc_demo;

drop table  "onsite_galaxy_postgres"."cdc_demo"."cdc_account";
create table "onsite_galaxy_postgres"."cdc_demo"."cdc_account" (
    account_id int,
    balance decimal(6,2),
    tid int,
    last_updated timestamp(6) with time zone 
);

-- SELECT CURRENT_TIMESTAMP(6) AT TIME ZONE 'UTC';

---Postgres Table
-- delete from "onsite_galaxy_postgres"."cdc_demo"."cdc_account" where account_id = 100001;
INSERT INTO "onsite_galaxy_postgres"."cdc_demo"."cdc_account" (account_id, balance, tid, last_updated)
VALUES
  (100001, 1000.31, 101, current_timestamp(6) at time zone 'UTC') ;

SELECT * FROM "onsite_galaxy_postgres"."cdc_demo"."cdc_account" LIMIT 10;

---CDC Changes Table
drop table demo_aws_s3.trino_iceberg_edu.cdc_account_changes;
create table demo_aws_s3.trino_iceberg_edu.cdc_account_changes  (
    operation varchar,
    account_id int,
    balance decimal(6,2),
    tid int,
    last_updated timestamp(6) with time zone 
);

INSERT INTO demo_aws_s3.trino_iceberg_edu.cdc_account_changes (operation, account_id, balance, tid, last_updated)
VALUES
  ('I', 100001, 1000.31, 101, current_timestamp(6) at time zone 'UTC') ;


SELECT * FROM "demo_aws_s3"."trino_iceberg_edu"."cdc_account_changes" LIMIT 10;

---Merge from Changes to Account table 
MERGE INTO cdc_accounts a USING cdc_account_changes c
ON a.account_id = c.account_id
WHEN MATCHED AND operation = 'D' THEN DELETE
WHEN MATCHED and a.last_updated < c.last_updated THEN UPDATE
    SET account_id = c.account_id,
        balance = c.balance,
        tid = c.tid,
        last_updated = c.last_updated
WHEN NOT MATCHED AND operation != 'D' THEN
    INSERT (account_id,   balance,   tid,   last_updated)
    VALUES (c.account_id, c.balance, c.tid, c.last_updated);

SELECT * FROM "demo_aws_s3"."trino_iceberg_edu"."cdc_accounts" LIMIT 10;

--Perform an Update 
update "onsite_galaxy_postgres"."cdc_demo"."cdc_account" set balance = 999.99 where account_id = 100001;

INSERT INTO demo_aws_s3.trino_iceberg_edu.cdc_account_changes (operation, account_id, balance, tid, last_updated)
VALUES
  ('U', 100001, 999.99, 101, current_timestamp(6) at time zone 'UTC') ;

--------- Metadata Columns & Tables -----------------------------------------------------------------------------

SELECT account_id, "$path", "$file_modified_time" FROM cdc_accounts;

SELECT * FROM "cdc_accounts$snapshots";
SELECT * FROM "cdc_accounts$history";
SELECT * FROM "cdc_accounts$manifests";
SELECT * FROM "cdc_accounts$partitions";
SELECT * FROM "cdc_accounts$files";
SELECT * FROM "cdc_accounts$refs";
