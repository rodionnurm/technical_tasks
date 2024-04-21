from src.user_tracking_logs.logs_settings.setup_logger import logger
from src.user_tracking_logs.db_connectors.postgresql import Postgres
import json
from pathlib import Path
import pandas as pd
from pandas import json_normalize
from src.user_tracking_logs.references_writer.writer import ReferenceWriter

# import authorization config
path_authorization_config = str(Path(Path(__file__).parents[2], 'authorization_config', 'config.json'))
with open(path_authorization_config) as config_file:
    config = json.load(config_file)
database, user, password, host, port = config['database'], config['user'], config['password'], config['host'], config['port']

# create connection to PostgreSQL
postger = Postgres()
Session, connection = postger.connector(database=database, user=user, password=password, host=host, port=port, logger=logger)

# read json file
path_json_file = str(Path(Path(Path(__file__).parents[2], 'files', 'clicks.json')))
with open(path_json_file, encoding="utf-8") as json_file:
    clicks = json.load(json_file)

df = json_normalize(clicks['data'])

df_device = df[['device_manufacturer', 'device_model', 'device_type']].drop_duplicates()
query = 'SELECT device_manufacturer, device_model, device_type FROM korzinka_db.c_device'
table = 'c_device'
ReferenceWriter.writer(connection, df_device, query, table)


