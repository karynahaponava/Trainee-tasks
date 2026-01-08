import pandas as pd
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / 'data' / 'raw' / 'database.csv'
OUTPUT_DIR = BASE_DIR / 'data' / 'monthly_data'

def split_csv_by_month():
    if not OUTPUT_DIR.exists():
        os.makedirs(OUTPUT_DIR)

    print(f"--- Начинаю разбивку файла: {INPUT_FILE} ---")
    
    chunk_size = 500000
    
    try:
        csv_reader = pd.read_csv(INPUT_FILE, chunksize=chunk_size, parse_dates=['departure'])
    except FileNotFoundError:
        print("ОШИБКА: Файл database.csv не найден!")
        print(f"Проверьте, что он лежит тут: {INPUT_FILE}")
        return

    batch_num = 0
    
    for chunk in csv_reader:
        batch_num += 1
        print(f"Обрабатываю часть №{batch_num}...")

        chunk['month_key'] = pd.to_datetime(chunk['departure'], errors='coerce').dt.to_period('M')

        for group_name, df_group in chunk.groupby('month_key'):
            if pd.isna(group_name):
                continue
                
            filename = f"bikes_{group_name}.csv"
            save_path = OUTPUT_DIR / filename
            
            write_header = not save_path.exists()
            
            df_group.drop(columns=['month_key']).to_csv(save_path, mode='a', index=False, header=write_header)

    print(f"--- Готово! Файлы лежат в {OUTPUT_DIR} ---")

if __name__ == "__main__":
    split_csv_by_month()