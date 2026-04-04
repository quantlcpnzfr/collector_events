from __future__ import annotations

from typing import Any

from collector_events.extractors.factory import ExtractorProviderFactory
from collector_events.extractors.provider import DatasetSplitInfo, ExtractorProvider


def create_extractor(provider_name: str, **kwargs: Any) -> ExtractorProvider:
    """Convenience wrapper around the extractor factory."""

    return ExtractorProviderFactory.create(provider_name, **kwargs)


def resolve_extractor_name(provider_name: str) -> str:
    """Return the canonical provider name for a requested alias or dataset id."""

    return ExtractorProviderFactory.resolve_provider_name(provider_name)


def available_extractors(include_aliases: bool = False) -> list[str]:
    """Return registered providers, optionally including aliases."""

    return ExtractorProviderFactory.available_providers(include_aliases=include_aliases)


__all__ = [
    "DatasetSplitInfo",
    "ExtractorProvider",
    "ExtractorProviderFactory",
    "available_extractors",
    "create_extractor",
    "resolve_extractor_name",
]
