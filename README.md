REST API - > Dataframe - > Pre-process Data - > Add columns for variability - > Randomize data for additional columns - > Map Columns to proper dtype - > Generate Column Data (Parquet File)

**Pre-process Data**
- Remove Duplicates
- Remove Erroneous Data

**Randomize data for additional columns**
- Creates date_accessed
- Creates date_loaded
- Randomize dates for date_accessed (For Analysis)
- Creates date_loaded for watermarking

**Parquet File (info_blogs.parquet)**
- Prepared data for loading to Google BigQuery/Postgres via Python script

**Libraries:**
(pandas, pyarrow, numpy)
