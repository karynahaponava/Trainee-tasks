import pandas as pd
from datetime import datetime, timedelta

columns = [
    "error_code",
    "error_message",
    "severity",
    "log_location",
    "mode",
    "model",
    "graphics",
    "session_id",
    "sdkv",
    "test_mode",
    "flow_id",
    "flow_type",
    "sdk_date",
    "publisher_id",
    "game_id",
    "bundle_id",
    "appv",
    "language",
    "os",
    "adv_id",
    "gdpr",
    "ccpa",
    "country_code",
    "date",
]

data = []
start_time = datetime.now()

for i in range(20):
    row = {col: None for col in columns}

    row["error_code"] = 500
    row["severity"] = "Error"
    row["severity"] = "Fatal"

    row["bundle_id"] = "com.test.bundle"
    row["os"] = "Android"

    row["date"] = start_time + timedelta(seconds=i)

    data.append(row)

import os

if not os.path.exists("./data"):
    os.makedirs("./data")

df = pd.DataFrame(data, columns=columns)
df.to_csv("./data/data.csv", index=False)

print("Файл data/data.csv создан! В нем 20 фатальных ошибок для теста.")
