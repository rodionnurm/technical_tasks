import pandas as pd

class ReferenceWriter:
    @staticmethod
    def writer(connection, df, query, table):
        connection.autocommit = True
        query = query
        df_device = pd.read_sql(query, connection)
        df_full = pd.concat([df_device, df], ignore_index=True).drop_duplicates()
        print(df_full)
        df_full.to_sql(name=table, con=connection, schema='korzinka_db', if_exists='append', index=False)
