import sys
import os
from unittest.mock import MagicMock
import pytest

os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

module_mock = MagicMock()
sys.modules["airflow"] = module_mock
sys.modules["airflow.operators"] = module_mock
sys.modules["airflow.operators.bash"] = module_mock
sys.modules["airflow.operators.python"] = module_mock
sys.modules["airflow.utils"] = module_mock
sys.modules["airflow.utils.dates"] = module_mock
