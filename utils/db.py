import os
import pandas as pd
import psycopg2
from typing import List
from sqlalchemy import create_engine


def load_df_to_postgress(df_list: [pd.DataFrame], table_names: List[str]):
    # establish connections
    conn_string = 'postgresql://postgres:admin@127.0.0.1/soccer_data_model'

    db = create_engine(conn_string)
    conn = db.connect()
    conn1 = psycopg2.connect(
        database=os.getenv("WAREHOUSE_DB"),
    user=os.getenv("WAREHOUSE_USER"), 
    password=os.getenv("WAREHOUSE_PASSWORD"), 
    host=os.getenv("WAREHOUSE_HOST"), 
    port= os.getenv("WAREHOUSE_PORT")
    )

    conn1.autocommit = True
    cursor = conn1.cursor()

    for i in range(len(df_list)):
        print("Loading data now")
        df = df_list[i]
        df.to_sql(table_names[i], conn, if_exists= 'replace', index=False)

    conn1.commit()
    conn1.close()