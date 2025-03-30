### 🏗️ Schema Optimization: Partitioning & Clustering
To enhance query performance and reduce costs, the main dataset is:
- ✅ Partitioned by date → Improves query efficiency by filtering certain range of dates
- ✅ Clustered (optional for further optimization) → Groups data by categorical column `SP500_daily_change_category`

### 📌 Creating the Partitioned Table
**Option 1**: Use the `bq` CLI:
```bash
cd BigQuery
bq query --use_legacy_sql=false -q < create_partitioned_table.sql
```

**Option 2**: Use **Query Editor** in **BigQuery** console and run the SQL query in [create_partitioned_table.sql](BigQuery/create_partitioned_table.sql).


### 📌 Creating the Partitioned and Clustered Table
**Option 1**: Use the `bq` CLI:
```bash
cd BigQuery
bq query --use_legacy_sql=false -q < create_partitioned_clustered_table.sql
```

**Option 2**: Use **Query Editor** in **BigQuery** console and run the SQL query in [create_partitioned_clustered_table.sql](BigQuery/create_partitioned_clustered_table.sql).


### 🔹How Partitioning & Clustering Work Together
- Partitioning (`date`) → First, BigQuery divides the data into partitions based on the date column.
- Clustering (`SP500_daily_change_category`) → Within each partition, BigQuery physically sorts and groups data by `SP500_daily_change_category`.
- Query Optimization:
  - Queries filtering by `date` scan only relevant partitions.
  - Queries filtering by `SP500_daily_change_category` within a partition run even faster due to clustering.