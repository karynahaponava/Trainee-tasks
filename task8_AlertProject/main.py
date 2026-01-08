import os
import time
from src.rules import FatalErrorTimeRule, BundleFatalErrorRule
from src.processor import LogProcessor

INPUT_DIR = './data'

def main():
    active_rules = [
        FatalErrorTimeRule(),
        BundleFatalErrorRule()
    ]

    processor = LogProcessor(active_rules)

    print("Система запущена. Ожидание файлов в папке data...")

    while True:
        if os.path.exists(INPUT_DIR):
            files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
            
            for file in files:
                filepath = os.path.join(INPUT_DIR, file)
                
                processor.process(filepath)
                
                new_path = filepath + ".processed"
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.rename(filepath, new_path)
                print(f"Файл {file} обработан и переименован.")
        
        time.sleep(3) 

if __name__ == "__main__":
    main()