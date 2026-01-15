import os
import pytest
import pandas as pd
from scripts.split_csv import split_csv_by_date


def test_split_csv_creates_files(tmp_path):
    """Проверяем, что скрипт разбивает один файл на два месяца."""
    d = {
        "Departure time": ["2020-05-01 10:00", "2020-05-02 11:00", "2020-06-01 12:00"],
        "data": [1, 2, 3],
    }
    df = pd.DataFrame(data=d)

    input_file = tmp_path / "test_input.csv"
    output_dir = tmp_path / "monthly"
    os.makedirs(output_dir, exist_ok=True)

    df.to_csv(input_file, index=False)

    split_csv_by_date(str(input_file), str(output_dir))

    files = os.listdir(output_dir)
    files.sort()

    assert len(files) == 2
    assert "bikes_2020-05.csv" in files
    assert "bikes_2020-06.csv" in files


def test_split_csv_invalid_file(tmp_path):
    """Проверяем, что скрипт падает (или выходит), если файла нет."""
    with pytest.raises(SystemExit):
        split_csv_by_date("non_existent_file.csv", str(tmp_path))
