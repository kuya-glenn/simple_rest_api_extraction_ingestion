import requests
import pandas as pd
import time
import re
import pyarrow
import logging
import json
import numpy as np
from datetime import datetime, timedelta

class JsonFormat(logging.Formatter):
    def format(self,record):
        recordLog = {
            "level": record.levelname,
            "time": self.formatTime(record, self.datefmt),
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            recordLog["exception"] = self.formatException(record.exc_info)
        return json.dumps(recordLog)
    
logger = logging.getLogger("IngestionAPI")
handler = logging.StreamHandler()
handler.setFormatter(JsonFormat())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

url = "https://api.slingacademy.com/v1/sample-data/blog-posts"
#pagination_param = {"offset": 0, "limit": 3}
blogs = []
output_dir = "info_blogs.parquet"

def fetchPage(offset, limit, retries=5, backoff=2):
    for tries in range(1, retries +1):
        try:
            logger.info(f"Fetching offset={offset}, limit={limit}, attept={tries}")
            response = requests.get(url, params={"offset": offset, "limit": limit}, timeout=5)
            response.raise_for_status()
            data = response.json()

            if response.status_code == 429:
                retry = int(response.headers.get("retry-in",1))
                logger.warning(f"429 Too many requests, retrying in {retry}s")
                print("Too many requests, try again")
                time.sleep(retry)
                continue
            
            if "detail" in data:
                msg = data["detail"]
                seconds_detail = re.search(r"(\d+)", msg)
                if seconds_detail:
                    retry = int(seconds_detail.group(1))
                else:
                    ## Adding fallback
                    retry = 10

                logger.warning(f"Rate limited by API, retrying in {retry}s")
                print(f"Rate limited, retrying in {retry}s")
                time.sleep(retry)
                continue

            ## Raise error if bad status
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Failed, try ({tries}/{retries}): {e}")
            print(f"Attempt no. {tries} faled: {e}")
            if tries < retries:
                sleepTime = backoff ** tries
                logger.info(f"Retrying in {sleepTime}s")
                print(f"Retrying in {sleepTime}s")
                time.sleep(sleepTime)
            else:
                logger.critical("Retires exhausted")
                print("Retries failed")
                return None

for offset in range(0,100,10):
    data = fetchPage(offset, 10)
    if data:
        blogs.extend(data.get("blogs",[]))

df = pd.DataFrame(blogs)

## Performing deduplication on pandas to reduce runtime
## Dataset: User has multiple post, performed deduplication to show the concept of deduplication
if "user_id" in df.columns:
    duplicated = df.shape[0]
    df = df.drop_duplicates(subset=["user_id"], keep="last")
    deduplicated = df.shape[0]
    #print(f"Rows remaining: {deduplicated} | Rows before dedupllication: {duplicated}")
    logger.info(f"Rows remaining: {deduplicated} | Rows before dedupllication: {duplicated}")
else:
    df = df.drop_duplicates()

assert not df.duplicated(subset=["user_id"]).any(), "Validation Failed: Duplicates are still present"

## Dropping created_at date to generate random dates for google big query loading
df = df.drop(columns=['created_at'], axis=1)
#print(df.head())
#print(df.dtypes)


## Adding a randomized data for Q2 in the assessment
start_date = datetime(2023, 3, 16, 0, 0, 0)
end_date = datetime.utcnow()

row_count = len(df)

random_sec = np.random.randint(0, int((end_date - start_date).total_seconds()), size = row_count)

df['created_at'] = [start_date + timedelta(seconds=int(s)) for s in random_sec]
df['random_users_count'] = np.random.randint(1, 101, size=len(df))
df["created_date"] =  pd.to_datetime(df["created_at"]).dt.normalize()
random_time = np.random.randint(0, 24*60*60, size=row_count)
df['date_accessed'] = [end_date -timedelta(days=i) for i in range(row_count)]
df['date_accessed'] = [d + timedelta(seconds=int(s)) for d, s in zip(df['date_accessed'], random_time)]
#df['date_accessed'] = pd.to_datetime(df["date_accessed"]).dt.normalize()

## Adding datae_loaded for Q3
import pytz

local_timezone = pytz.timezone("Asia/Manila")
now = datetime.now()
df['date_loaded'] = now
mask = np.random.rand(row_count) < 0.5
df.loc[mask, 'date_loaded'] = now - timedelta(minutes=5)
df['date_loaded'] = pd.to_datetime(df['date_loaded'])
#df['date_loaded'] = np.where(mask, now - timedelta(minutes=5), now)

## Map datatypes
datatype_map = {
    "user_id": "int64",
    "title": "string",
    "context_text": "string",
    "photo_url": "string",
    "description": "string",
    "id": "int64",
    "content_html": "string",
    "category": "string",
    "created_at": "datetime64[ns]",
    "updated_at": "datetime64[ns]",
    "date_accessed": "datetime64[ns]",
    "created_date": "datetime64[ns]",
    "random_users_count": "int64",
    "date_loaded": "datetime64[ns]"
    }

for col, dtype in datatype_map.items():
    if col in df.columns:
        if "datetime" in dtype:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].astype("datetime64[us]")
        elif dtype == "string":
            df[col] = df[col].astype("string")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype, errors="ignore")

#df['date_accessed'] = df['date_accessed'].dt.date

#df.to_csv('literature.csv',index=False)
df['created_date'] = df['created_date'].dt.date
#print(df.head())
print(df.head)

## Saving to parquet format 
df.to_parquet(output_dir, engine="pyarrow", index=False)
print(df.dtypes)

logger.info(f"Data saved to {output_dir} in parquet format, partitioned")