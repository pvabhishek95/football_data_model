import os
import pandas as pd
import psycopg2
from typing import List
from sqlalchemy import create_engine


def load_df_to_postgress(df_list: [pd.DataFrame], table_names: List[str]):
    # establish connections
    conn_string = 'postgresql://admin:admin@warehouse:5432/soccer_data_model'

    db = create_engine(conn_string)
    conn = db.connect()

    for i in range(len(df_list)):
        print(f"Creating table {table_names[i]}")
        df = df_list[i]
        df.to_sql(table_names[i], conn, if_exists= 'replace', index=False)