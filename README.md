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

 <img width="975" height="187" alt="image" src="https://github.com/user-attachments/assets/26ebb982-1d78-4a97-b475-0c785998f868" />


Process:

<img width="552" height="1246" alt="image" src="https://github.com/user-attachments/assets/6efc4cb9-c65a-4621-9e14-110948f828d0" />
<img width="446" height="1247" alt="image" src="https://github.com/user-attachments/assets/21a600a4-c33a-4206-9ae5-1d6129326c23" />


**Libraries:**
(pandas, pyarrow, numpy)
