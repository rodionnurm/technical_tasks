import pandas as pd
import numpy as np


class Writer:
    @staticmethod
    def reference_writer(engine, df, query, table, logger):
        with engine.connect() as connection:
            query = query
            connection.begin()
            try:
                df_reference = pd.read_sql(query, connection)
                df_full = pd.concat([df_reference, df], ignore_index=True).drop_duplicates(keep=False)
                df_full.to_sql(name=table, con=connection, schema='korzinka_db', if_exists='append', index=False)
                logger.info(f'successfully loaded data to table {table}')
            except Exception as e:
                connection.rollback()
                logger.error(f'failed load data to table {table}: {e}')
            else:
                connection.commit()

    @staticmethod
    def user_tracking_logs_writer(engine, df, logger):
        query_device = 'SELECT id AS device_id, device_manufacturer, device_model, device_type FROM korzinka_db.c_device'
        query_location = 'SELECT id AS location_id, country_iso_code, city FROM korzinka_db.c_location'
        query_os = 'SELECT id AS os_id, os_name, os_version FROM korzinka_db.c_os'
        query_user_tracking_logs = '''SELECT application_id, publisher_id, tracking_id, click_timestamp, click_datetime,
                                             click_ipv6, click_url_parameters, click_id, click_user_agent, ios_ifa, 
                                             ios_ifv, android_id, google_aid, is_bot, device_id, location_id, os_id 
                                             FROM korzinka_db.user_tracking_logs'''

        with engine.connect() as connection:
            connection.begin()
            try:
                df_device = pd.read_sql(query_device, connection)
                df_location = pd.read_sql(query_location, connection)
                df_os = pd.read_sql(query_os, connection)

                df.click_timestamp = pd.to_datetime(df.click_timestamp, unit='s')

                df_full = pd.merge(df, df_device, how='inner',
                                   on=['device_manufacturer', 'device_model', 'device_type'])
                df_full = pd.merge(df_full, df_location, how='inner',
                                   on=['country_iso_code', 'city'])
                df_full = pd.merge(df_full, df_os, how='inner',
                                   on=['os_name', 'os_version'])

                df_full = df_full.drop(['device_manufacturer', 'device_model', 'device_type',
                                              'country_iso_code', 'city', 'os_name', 'os_version',
                                              'publisher_name', 'tracker_name'], axis=1)

                df_user_tracking_logs = pd.read_sql(query_user_tracking_logs, connection)

                df_full.application_id = df_full.application_id.astype(np.int64)
                df_full.click_datetime = pd.to_datetime(df_full.click_datetime)
                df_full.is_bot = df_full.is_bot.astype(bool)

                df_full = df_full.drop_duplicates()
                df_full = pd.concat([df_full, df_user_tracking_logs], ignore_index=True).drop_duplicates(keep=False)


                df_full.to_sql(name='user_tracking_logs', con=connection, schema='korzinka_db', if_exists='append', index=False)
                logger.info(f'successfully loaded data to table korzinka_db.user_tracking_logs')
            except Exception as e:
                connection.rollback()
                logger.error(f'failed load data to table korzinka_db.user_tracking_logs: {e}')
            else:
                connection.commit()
