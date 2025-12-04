import pandas as pd
import numpy as np
import random
import os

SOURCE_FILE = 'Sample - Superstore.csv' 
OUTPUT_DIR = 'data'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print(f"1. Читаем файл '{SOURCE_FILE}'...")

try:
    df = pd.read_csv(SOURCE_FILE)
except:
    df = pd.read_csv(SOURCE_FILE, encoding='windows-1252')

if len(df) == 0:
    raise ValueError("Файл пустой или не прочитался!")

if 'Row ID' in df.columns:
    df['Row ID'] = df['Row ID'].astype(str)

print(f"   Всего строк: {len(df)}")

np.random.seed(42)
initial_mask = np.random.rand(len(df)) < 0.8

df_initial = df[initial_mask].copy()
df_remainder = df[~initial_mask].copy()


df_new = df_remainder.sample(frac=0.5, random_state=42).copy()

df_dups = df_initial.sample(n=50, random_state=42).copy()

df_scd1 = df_initial.sample(n=20, random_state=1).copy()
df_scd1['Customer Name'] = df_scd1['Customer Name'] + " (Updated)"

df_scd2 = df_initial.sample(n=20, random_state=2).copy()
regions = ['South', 'West', 'Central', 'East']

def change_region(current_region):
    choices = [r for r in regions if r != current_region]
    return random.choice(choices)

df_scd2['Region'] = df_scd2['Region'].apply(change_region)

df_secondary = pd.concat([df_new, df_dups, df_scd1, df_scd2])
df_secondary = df_secondary.sample(frac=1, random_state=42).reset_index(drop=True)

print("2. Сохраняем результаты в папку data...")
df_initial.to_csv(f'{OUTPUT_DIR}/initial_load.csv', index=False)
df_secondary.to_csv(f'{OUTPUT_DIR}/secondary_load.csv', index=False)

print("-" * 30)
print("ГОТОВО! Проверь папку 'data'. Там должны появиться 2 файла.")