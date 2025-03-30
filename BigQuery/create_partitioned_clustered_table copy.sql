-- create partitioned and clustered table
CREATE OR REPLACE TABLE ecopulse_bq_dw.ecopulse_merged_partitioned_clustered
PARTITION BY date
CLUSTER BY SP500_daily_change_category
AS
SELECT * FROM ecopulse_bq_dw.ecopulse_merged;