# globalintel/global_tag_manager.py
"""
Backward-compatibility shim — real implementation moved to tagging/ sub-package.

Import GlobalTagManager from here or from collector_events.globalintel.tagging.
"""

from collector_events.globalintel.tagging import GlobalTagManager  # noqa: F401

__all__ = ["GlobalTagManager"]
