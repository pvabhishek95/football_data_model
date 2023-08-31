from typing import List
from sqlalchemy import create_engine
import psycopg2
import pandas as pd


def load_df_to_postgress(df_list: [pd.DataFrame], table_names: List[str]):
    # establish connections
    conn_string = 'postgresql://postgres:admin@127.0.0.1/soccer_data_model'

    db = create_engine(conn_string)
    conn = db.connect()
    conn1 = psycopg2.connect(
        database="soccer_data_model",
    user='postgres', 
    password='admin', 
    host='localhost', 
    port= '5432'
    )

    conn1.autocommit = True
    cursor = conn1.cursor()

    for i in range(len(df_list)):
        print("Loading data now")
        df = df_list[i]
        df.to_sql(table_names[i], conn, if_exists= 'replace', index=False)

    conn1.commit()
    conn1.close()