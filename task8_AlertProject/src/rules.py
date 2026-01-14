from abc import ABC, abstractmethod
from typing import List
import pandas as pd


class BaseAlertRule(ABC):
    """Abstract base class for all alert rules."""

    @abstractmethod
    def check(self, df: pd.DataFrame) -> List[str]:
        pass


class FatalErrorTimeRule(BaseAlertRule):
    """Rule 2.1: Detects more than 10 fatal errors within a rolling 1-minute window."""

    def check(self, df: pd.DataFrame) -> List[str]:
        alerts = []

        fatal_df = df[df["severity"] == "Fatal"].copy()

        if fatal_df.empty:
            return alerts

        if not isinstance(fatal_df.index, pd.DatetimeIndex):
            fatal_df = fatal_df.set_index("date")

        fatal_df.sort_index(inplace=True)

        rolling_counts = fatal_df.rolling("1min").count()["severity"]

        spikes = rolling_counts[rolling_counts > 10]

        if not spikes.empty:
            resampled_view = spikes.resample("1min").max().dropna()

            for time, count in resampled_view.items():
                alerts.append(
                    f"[RULE 2.1] Critical: {int(count)} fatal errors around {time}"
                )

        return alerts


class BundleFatalErrorRule(BaseAlertRule):
    """Rule 2.2: Detects more than 10 fatal errors for a specific bundle_id within a rolling 1-hour window."""

    def check(self, df: pd.DataFrame) -> List[str]:
        alerts = []

        fatal_df = df[df["severity"] == "Fatal"].copy()

        if fatal_df.empty:
            return alerts

        if not isinstance(fatal_df.index, pd.DatetimeIndex):
            fatal_df = fatal_df.set_index("date")

        fatal_df.sort_index(inplace=True)

        grouped = fatal_df.groupby("bundle_id")["severity"].rolling("1h").count()

        spikes = grouped[grouped > 10]

        if not spikes.empty:
            problematic_bundles = spikes.index.get_level_values("bundle_id").unique()

            for bundle in problematic_bundles:
                alerts.append(
                    f"[RULE 2.2] Bundle Alert: High fatal rate detected for {bundle}"
                )

        return alerts
