from utils.dataIngestFromGCS import ingestion_initialization
# from utils.configLoader import load_config
from utils.logging import log_object_creation

# from utils.logging import 
def main():
    # Creating log object which will be used across the modules
    logger = log_object_creation()
    logger.info("main.py| Application started.")
    #app_log = 

    # Initializing the config for GCS data bucket to access the data shared
    #gcs_config = load_config()
    
    # Calling the data ingestion function from GCS to local system for temp storage
    logger.info("main.py| Starting data ingestion from GCS to local storage.")
    try:
        if ingestion_initialization(logger):
            logger.info("main.py| Data ingestion process completed.")

    except Exception as e:
        logger.error(f"main.py| Data ingestion process failed, check this error and fix it: {e}")

    # Data movement from local temp storage to DB can be added here

if __name__ == "__main__":
    main()
