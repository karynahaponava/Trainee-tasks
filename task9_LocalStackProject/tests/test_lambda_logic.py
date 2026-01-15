import unittest
from scripts.lambda_function import get_target_table


class TestLambdaRouting(unittest.TestCase):

    def test_daily_metrics_route(self):
        """Проверяем, что файлы daily идут в DailyMetrics"""
        key = "s3://bucket/metrics/daily/part-0000.csv"
        result = get_target_table(key)
        self.assertEqual(result, "DailyMetrics")

    def test_departures_route(self):
        """Проверяем, что departures идут в DeparturesData"""
        key = "metrics/departures/part-0000.csv"
        result = get_target_table(key)
        self.assertEqual(result, "DeparturesData")

    def test_returns_route(self):
        """Проверяем, что returns идут в ReturnsData"""
        key = "some/path/metrics/returns/data.csv"
        result = get_target_table(key)
        self.assertEqual(result, "ReturnsData")

    def test_unknown_file(self):
        """Проверяем, что левые файлы игнорируются"""
        key = "random/folder/image.jpg"
        result = get_target_table(key)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
