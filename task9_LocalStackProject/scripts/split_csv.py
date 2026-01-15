import sys
import os
import pandas as pd


def split_csv_by_date(input_file, output_dir):
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    try:
        df = pd.read_csv(input_file)

        df["dt"] = pd.to_datetime(df["Departure time"])

        for period, group in df.groupby(df["dt"].dt.to_period("M")):
            month_str = str(period)
            filename = f"bikes_{month_str}.csv"
            save_path = os.path.join(output_dir, filename)

            group.drop(columns=["dt"]).to_csv(save_path, index=False)
            print(f"Saved {save_path}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python split_csv.py <input_file> <output_dir>")
        sys.exit(1)
    split_csv_by_date(sys.argv[1], sys.argv[2])
