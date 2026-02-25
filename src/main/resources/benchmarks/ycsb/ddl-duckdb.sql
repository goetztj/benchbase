INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;
DROP TABLE IF EXISTS usertable;

ATTACH DATABASE 'ducklake:postgres:dbname=ducklake_catalog host=localhost user=duck password=duckpass'
AS my_ducklake_ycsb (
	DATA_PATH 's3://warehouse/duckdb/',
    OVERRIDE_DATA_PATH true
);

use my_ducklake_ycsb;

DROP TABLE IF EXISTS usertable;

CREATE TABLE usertable (
    ycsb_key int,
    field1   text,
    field2   text,
    field3   text,
    field4   text,
    field5   text,
    field6   text,
    field7   text,
    field8   text,
    field9   text,
    field10  text
);
