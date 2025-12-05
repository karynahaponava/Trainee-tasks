import pandas as pd
from typing import List
from .rules import BaseAlertRule

COLUMNS_NAMES = [
    'error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 
    'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 
    'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 'appv', 'language', 
    'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date'
]

class LogProcessor:
    def __init__(self, rules: List[BaseAlertRule]):
        self.rules = rules

    def process(self, filepath: str):
        print(f"Processing {filepath}...")
        
        try:
            chunk_size = 100000 
            
            with pd.read_csv(filepath, 
                             header=0, 
                             names=COLUMNS_NAMES, 
                             chunksize=chunk_size, 
                             low_memory=False) as reader:
                
                for chunk_df in reader:
                    chunk_df['date'] = pd.to_datetime(chunk_df['date'], unit='s', errors='coerce')
                    
                    self._apply_rules(chunk_df, filepath)
                    
            print(f"Файл {filepath} полностью обработан.")
                
        except Exception as e:
            print(f"Critical Error processing {filepath}: {e}")

    def _apply_rules(self, df: pd.DataFrame, filename: str):
        all_alerts = []
        for rule in self.rules:
            rule_alerts = rule.check(df)
            all_alerts.extend(rule_alerts)
        
        if all_alerts:
            print(f"\n--- АЛЕРТЫ (файл {filename}) ---")
            for alert in all_alerts:
                print(alert)
            print("------------------------------\n")