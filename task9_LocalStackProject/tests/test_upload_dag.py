import pytest
from unittest.mock import patch, MagicMock
from dags.upload_to_s3_dag import upload_source_data


@patch("dags.upload_to_s3_dag.boto3.client")
@patch("dags.upload_to_s3_dag.os.path.exists")
def test_upload_source_success(mock_exists, mock_boto_client):
    """Проверяем успешную загрузку."""
    mock_exists.return_value = True

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    upload_source_data()

    mock_s3.upload_file.assert_called_once()


@patch("dags.upload_to_s3_dag.os.path.exists")
def test_upload_source_no_file(mock_exists):
    """Проверяем, что функция кидает ошибку, если файла нет."""
    mock_exists.return_value = False

    with pytest.raises(FileNotFoundError):
        upload_source_data()
