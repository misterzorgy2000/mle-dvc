## scripts/data.py

# 1 — импорты
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import yaml

# 2 — вспомогательные функции
def create_connection():

    load_dotenv()
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    
    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}', connect_args={'sslmode':'require'})
    return conn

def fill_missing_values(data):
    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')
    for col in cols_with_nans:
        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]
        data[col] = data[col].fillna(fill_value)
    return data

# 3 — главная функция
def get_data():
    # 3.1 — загрузка гиперпараметров
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
    conn = create_connection()
    data = pd.read_sql('select * from clean_users_churn', conn, index_col=params['index_col'])
    data = fill_missing_values(data)
    
    conn.dispose()

    # 3.4 — сохранение результата шага
    os.makedirs('data', exist_ok=True)
    data.to_csv('data/initial_data.csv', index=None)

# 4 — защищённый вызов главной функции
if __name__ == '__main__':
    get_data()