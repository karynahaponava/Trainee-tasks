import pandas as pd
from typing import List
import os
from .rules import BaseAlertRule
from .logger import logger
from .metrics import metrics

REQUIRED_COLUMNS = {"severity", "date", "bundle_id"}

COLUMNS_NAMES = [
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


class LogProcessor:
    def __init__(self, rules: List[BaseAlertRule]):
        self.rules = rules

    def process(self, filepath: str):
        """Reads CSV in chunks, validates data, and applies alert rules."""
        logger.info(f"Processing started: {filepath}")

        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            metrics.increment("errors_file_not_found")
            return

        try:
            chunk_size = 100000

            with pd.read_csv(
                filepath,
                header=None,
                names=COLUMNS_NAMES,
                chunksize=chunk_size,
                low_memory=False,
                on_bad_lines="warn",
                encoding="utf-8",
            ) as reader:

                total_processed_rows = 0

                for chunk_df in reader:
                    if chunk_df.empty:
                        continue

                    missing_cols = REQUIRED_COLUMNS - set(chunk_df.columns)
                    if missing_cols:
                        logger.error(f"Missing required columns: {missing_cols}")
                        metrics.increment("errors_validation_failed")
                        continue

                    chunk_df["date"] = pd.to_datetime(
                        chunk_df["date"], unit="s", errors="coerce"
                    )

                    initial_count = len(chunk_df)
                    chunk_df.dropna(subset=["date"], inplace=True)
                    dropped_count = initial_count - len(chunk_df)

                    if dropped_count > 0:
                        logger.warning(
                            f"Dropped {dropped_count} rows due to invalid dates"
                        )
                        metrics.increment("rows_dropped_invalid_date", dropped_count)

                    self._apply_rules(chunk_df, filepath)

                    total_processed_rows += len(chunk_df)

                metrics.increment("files_processed_success")
                metrics.increment("rows_processed_total", total_processed_rows)
                logger.info(
                    f"Finished processing {filepath}. Total valid rows: {total_processed_rows}"
                )

        except pd.errors.EmptyDataError:
            logger.warning(f"File is empty: {filepath}")
            metrics.increment("errors_empty_file")
        except Exception as e:
            logger.exception(f"Critical error processing {filepath}")
            metrics.increment("errors_critical_processing")

    def _apply_rules(self, df: pd.DataFrame, filename: str):
        for rule in self.rules:
            try:
                alerts = rule.check(df)
                for alert in alerts:
                    logger.warning(f"ALERT: {alert} | Source: {filename}")
                    metrics.increment("alerts_triggered")
            except Exception as e:
                logger.error(f"Error applying rule {type(rule).__name__}: {e}")
                metrics.increment("errors_rule_execution")
