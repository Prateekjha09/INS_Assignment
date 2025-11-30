import pandas as pd
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from google.cloud import storage
import time
import gc

# ---------- CONFIG ----------
PROJECT_ID = "inshortsassignment-479520"
BUCKET_NAME = "nis_interview_task_de"
USER_PREFIX = "user/"
EVENT_PREFIX = "event/" 
CONTENT_PREFIX = "content/"
MAX_THREADS = 20
# ----------------------------

class RawGCSLoader:
    def __init__(self, project_id: str):
        self.client = storage.Client(project_id)
        self.client._http._client_info = None  # Speed optimization
    
    def list_csv_blobs(self, prefix: str) -> List[storage.Blob]:
        bucket = self.client.bucket(BUCKET_NAME)
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=100))
        csv_blobs = [b for b in blobs if 'part' in b.name and b.name.endswith('.csv')]
        return csv_blobs
    
    def _raw_csv_loader(self, blob: storage.Blob) -> pd.DataFrame:
        """RAW CSV → DataFrame (NO dtype changes)"""
        try:
            content = blob.download_as_bytes()
            # NO dtype conversion - pure raw read
            df = pd.read_csv(
                BytesIO(content),
                dtype=str,  # ALL columns as strings
                low_memory=False
            )
            return df
        except Exception:
            return pd.DataFrame()
    
    def load_dataset(self, prefix: str, max_workers: int = MAX_THREADS) -> pd.DataFrame:
        """Concurrent raw load → NO transformations"""
        start_time = time.time()
        
        blobs = self.list_csv_blobs(prefix)
        if not blobs:
            raise ValueError(f"No CSV files under {prefix}")
        
        dfs = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._raw_csv_loader, blob) for blob in blobs]
            
            for i, future in enumerate(as_completed(futures), 1):
                df = future.result()
                if not df.empty:
                    dfs.append(df)
                if i % 5 == 0:
                    gc.collect()
        
        if not dfs:
            raise ValueError(f"No data from {prefix}")
        
        # Raw concat - NO type changes
        result = pd.concat(dfs, ignore_index=True, copy=False)
        return result


def ingestion_initialization(logger) -> bool:
    """Execute raw GCS → Parquet pipeline."""


    loader = RawGCSLoader(PROJECT_ID)
    
    # 1. USER - RAW LOAD
    logger.info("dataIngestFromGCS.py| Starting USER data ingestion.")
    user_df = loader.load_dataset(USER_PREFIX, max_workers=8)
    user_df.to_parquet("C:/Users/jha09/Downloads/InShorts_Assignment/INS_Assignment/SQL_Scripts/TableCreation/dataFiles/user.parquet", index=False)
    del user_df; gc.collect()
    logger.info("dataIngestFromGCS.py| Completed USER data ingestion.")
    
    # 2. CONTENT - RAW LOAD
    logger.info("dataIngestFromGCS.py| Starting CONTENT data ingestion.")
    content_df = loader.load_dataset(CONTENT_PREFIX, max_workers=8)
    content_df.to_parquet("C:/Users/jha09/Downloads/InShorts_Assignment/INS_Assignment/SQL_Scripts/TableCreation/dataFiles/content.parquet", index=False)
    del content_df; gc.collect()
    logger.info("dataIngestFromGCS.py| Completed CONTENT data ingestion.")
    
    # 3. EVENTS - RAW LOAD
    logger.info("dataIngestFromGCS.py| Starting EVENTS data ingestion.")
    event_df = loader.load_dataset(EVENT_PREFIX, max_workers=MAX_THREADS)
    event_df.to_parquet("C:/Users/jha09/Downloads/InShorts_Assignment/INS_Assignment/SQL_Scripts/TableCreation/dataFiles/event.parquet", index=False)
    del event_df; gc.collect()
    logger.info("dataIngestFromGCS.py| Completed EVENTS data ingestion.")
    
    return True
