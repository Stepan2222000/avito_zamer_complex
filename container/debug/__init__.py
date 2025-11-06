"""
Debug utilities for Avito parser workers.

This module provides debugging tools like screenshot capture
that can be enabled/disabled via environment variables.
"""

from container.debug.screenshot import debug_screenshot

__all__ = ["debug_screenshot"]
