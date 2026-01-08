from abc import ABC, abstractmethod
import pandas as pd
from typing import List

class BaseAlertRule(ABC):
    """
    Базовый абстрактный класс. 
    Все новые правила должны наследоваться от него.
    """
    @abstractmethod
    def check(self, df: pd.DataFrame) -> List[str]:
        pass

class FatalErrorTimeRule(BaseAlertRule):
    """
    Правило 2.1: Более 10 фатальных ошибок за 1 минуту.
    """
    def check(self, df: pd.DataFrame) -> List[str]:
        alerts = []
        fatal_df = df[df['severity'].isin(['Fatal', 'Error'])].copy()
        if fatal_df.empty:
            return alerts

        if not isinstance(fatal_df.index, pd.DatetimeIndex):
            fatal_df.set_index('date', inplace=True)

        counts = fatal_df.resample('1min').size()
        spikes = counts[counts > 10]

        for time, count in spikes.items():
            alerts.append(f"[RULE 2.1] Критично: {count} ошибок в {time}")
        
        return alerts

class BundleFatalErrorRule(BaseAlertRule):
    """
    Правило 2.2: Более 10 фатальных ошибок за 1 час для bundle_id.
    """
    def check(self, df: pd.DataFrame) -> List[str]:
        alerts = []
        fatal_df = df[df['severity'] == 'Fatal'].copy()
        if fatal_df.empty:
            return alerts

        if not isinstance(fatal_df.index, pd.DatetimeIndex):
            fatal_df.set_index('date', inplace=True)

        grouped = fatal_df.groupby('bundle_id')['severity'].resample('1h').count()
        spikes = grouped[grouped > 10]

        for (bundle_id, time), count in spikes.items():
            alerts.append(f"[RULE 2.2] Bundle Alert: {bundle_id} -> {count} ошибок в {time}")

        return alerts