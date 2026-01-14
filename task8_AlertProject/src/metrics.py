from dataclasses import dataclass, field
from typing import Dict
from .logger import logger


@dataclass
class MetricsRegistry:
    """Simple in-memory metrics collector.In a real production environment, this would push data to Prometheus or Datadog."""

    counters: Dict[str, int] = field(default_factory=dict)

    def increment(self, name: str, value: int = 1):
        """Increments a counter."""
        current = self.counters.get(name, 0)
        self.counters[name] = current + value
        logger.info(
            f"[METRIC] {name} increased by {value}. Total: {self.counters[name]}"
        )

    def get_metric(self, name: str) -> int:
        return self.counters.get(name, 0)


metrics = MetricsRegistry()
