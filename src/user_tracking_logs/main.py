from src.user_tracking_logs.logs_settings.setup_logger import logger
from src.user_tracking_logs.db_connectors.postgresql import Postgres
import json
from pathlib import Path
import pandas as pd
from pandas import json_normalize
from src.user_tracking_logs.db_writer.writer import Writer
import numpy as np

# import authorization config
path_authorization_config = str(Path(Path(__file__).parents[2], 'authorization_config', 'config.json'))
with open(path_authorization_config) as config_file:
    config = json.load(config_file)
database, user, password, host, port = config['database'], config['user'], config['password'], config['host'], config['port']

# create connection to PostgreSQL
postger = Postgres()
Session, engine = postger.connector(database=database, user=user, password=password, host=host, port=port, logger=logger)

# read json file
path_json_file = str(Path(Path(Path(__file__).parents[2], 'files', 'clicks.json')))
with open(path_json_file, encoding="utf-8") as json_file:
    clicks = json.load(json_file)

# create pandas df
df = json_normalize(clicks['data'])
df.publisher_id = df.publisher_id.astype(np.int64)
df.tracking_id = df.tracking_id.astype(np.int64)

# load reference c_device
df_device = df[['device_manufacturer', 'device_model', 'device_type']].drop_duplicates()
query = 'SELECT device_manufacturer, device_model, device_type FROM korzinka_db.c_device'
table = 'c_device'
Writer.reference_writer(engine, df_device, query, table, logger)

# load reference c_location
df_location = df[['country_iso_code', 'city']].drop_duplicates()
query = 'SELECT country_iso_code, city FROM korzinka_db.c_location'
table = 'c_location'
Writer.reference_writer(engine, df_location, query, table, logger)

# load reference c_os
df_os = df[['os_name', 'os_version']].drop_duplicates()
query = 'SELECT os_name, os_version FROM korzinka_db.c_os'
table = 'c_os'
Writer.reference_writer(engine, df_os, query, table, logger)

# load reference c_publisher
df_publisher = df[['publisher_id', 'publisher_name']].drop_duplicates()
query = 'SELECT publisher_id, publisher_name FROM korzinka_db.c_publisher'
table = 'c_publisher'
Writer.reference_writer(engine, df_publisher, query, table, logger)

# load reference c_track
df_track = df[['tracking_id', 'tracker_name']].drop_duplicates()
query = 'SELECT tracking_id, tracker_name FROM korzinka_db.c_track'
table = 'c_track'
Writer.reference_writer(engine, df_track, query, table, logger)

# load user_tracking_logs
Writer.user_tracking_logs_writer(engine, df, logger)
