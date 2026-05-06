"""Rule-based Intercompany (IC) resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable

from poc_ankrag.matching import normalize_vendor


@dataclass(frozen=True)
class VendorICMapping:
    vendor: str
    ic: str
    source: str
    effective_from: date | None = None
    effective_to: date | None = None
    is_active: bool = True

    @property
    def normalized_vendor(self) -> str:
        return normalize_vendor(self.vendor)

    def is_effective(self, as_of: date) -> bool:
        if not self.is_active:
            return False
        if self.effective_from is not None and as_of < self.effective_from:
            return False
        if self.effective_to is not None and as_of > self.effective_to:
            return False
        return True


@dataclass(frozen=True)
class HistoricalICUsage:
    vendor: str
    ic: str
    usage_count: int
    usage_share: float

    @property
    def normalized_vendor(self) -> str:
        return normalize_vendor(self.vendor)


@dataclass(frozen=True)
class ICResolution:
    ic: str
    source: str
    confidence: float


def resolve_ic(
    vendor: str,
    mappings: Iterable[VendorICMapping],
    historical_usage: Iterable[HistoricalICUsage],
    *,
    as_of: date | None = None,
    majority_threshold: float = 0.80,
    default_ic: str = "",
) -> ICResolution:
    """Resolve IC using deterministic mappings before historical fallback."""

    normalized_vendor = normalize_vendor(vendor)
    effective_date = as_of or date.today()

    for mapping in mappings:
        if (
            mapping.normalized_vendor == normalized_vendor
            and mapping.is_effective(effective_date)
            and mapping.ic
        ):
            return ICResolution(
                ic=mapping.ic,
                source=f"mapping:{mapping.source}",
                confidence=1.0,
            )

    vendor_usage = [
        usage
        for usage in historical_usage
        if usage.normalized_vendor == normalized_vendor and usage.ic
    ]
    if vendor_usage:
        best_usage = max(vendor_usage, key=lambda usage: (usage.usage_share, usage.usage_count))
        if best_usage.usage_share >= majority_threshold:
            return ICResolution(
                ic=best_usage.ic,
                source="historical_majority",
                confidence=best_usage.usage_share,
            )

    return ICResolution(ic=default_ic, source="default_non_ic", confidence=0.0)
