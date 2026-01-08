import pandas as pd
import re
from airflow.providers.mongo.hooks.mongo import MongoHook
from helpers.config import SOURCE_FILE, PROCESSED_FILE, TEMP_STEP1, TEMP_STEP2

def check_file_content(**kwargs):
    try:
        df = pd.read_csv(SOURCE_FILE)
        if df.empty:
            return 'file_is_empty'
        return 'processing_group.clean_nulls'
    except Exception:
        return 'file_is_empty'

def replace_nulls():
    df = pd.read_csv(SOURCE_FILE)
    df.fillna('-', inplace=True)
    df.to_csv(TEMP_STEP1, index=False)

def sort_data():
    df = pd.read_csv(TEMP_STEP1)
    if 'created_date' in df.columns:
        df.sort_values(by='created_date', inplace=True)
    df.to_csv(TEMP_STEP2, index=False)

def clean_text():
    df = pd.read_csv(TEMP_STEP2)
    
    def remove_special_chars(text):
        if not isinstance(text, str): return text
        return re.sub(r'[^\w\s.,?!-]', '', text)

    if 'content' in df.columns:
        df['content'] = df['content'].apply(remove_special_chars)
    
    df.to_csv(PROCESSED_FILE, index=False)

def upload_to_mongo():
    df = pd.read_csv(PROCESSED_FILE)
    data_dict = df.to_dict('records')

    hook = MongoHook(conn_id='mongo_default')
    hook.insert_many(mongo_collection='posts', docs=data_dict, mongo_db='airflow_db')
    print(f"Inserted {len(data_dict)} records into MongoDB")