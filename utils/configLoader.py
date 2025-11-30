import configparser
import os
from typing import NamedTuple


class MySQLConfig(NamedTuple):
    host: str
    user: str
    password: str
    database: str
    port: int


class GCSConfig(NamedTuple):
    project_id: str
    bucket_name: str
    user_prefix: str
    event_prefix: str
    content_prefix: str


def load_config(config_path: str = "C:/Users/jha09/Downloads/InShorts_Assignment/INS_Assignment/config/config.ini") -> tuple[MySQLConfig | None, GCSConfig]:
    """Load and return MySQL and GCS config values."""
    
    # Check if config file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    # Load config
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    
    #mysql_config = None
    gcs_config = None
    
    # MySQL Config (commented out - enable when needed)
    """if 'mySQL_Connection' in config:
        mysql = config['mySQL_Connection']
        mysql_config = MySQLConfig(
            host=mysql['host'],
            user=mysql['user'],
            password=mysql['password'],
            database=mysql['database'],
            port=int(mysql['port'])
        )"""
    
    # GCS Config
    if 'GCS_Connection' in config:
        gcs = config['GCS_Connection']
        gcs_config = GCSConfig(
            project_id=gcs['PROJECT_ID'],
            bucket_name=gcs['BUCKET_NAME'],
            user_prefix=gcs['USER_PREFIX'],
            event_prefix=gcs['EVENT_PREFIX'],
            content_prefix=gcs['CONTENT_PREFIX']
        )
    
    #return mysql_config, gcs_config
    return gcs_config


def get_gcs_config(config_path: str = "C:/Users/jha09/Downloads/InShorts_Assignment/INS_Assignment/config/config.ini") -> GCSConfig:
    """Get only GCS config."""
    _, gcs_config = load_config(config_path)
    return gcs_config


def get_mysql_config(config_path: str = "C:/Users/jha09/Downloads/InShorts_Assignment/INS_Assignment/config/config.ini") -> MySQLConfig:
    """Get only MySQL config."""
    mysql_config, _ = load_config(config_path)
    return mysql_config


# Production execution
#if __name__ == "__main__":
    #mysql_config, gcs_config = load_config()
    #gcs_config = load_config()
    #print(gcs_config.)